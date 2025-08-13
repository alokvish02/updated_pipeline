import asyncio
import argparse
import datetime
import multiprocessing
import threading
from time import sleep
import redis
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Pool, cpu_count
from services.data_lake.fyersdata import fyers_Symbol_gap_filler
from services.data_lake.crypto_spreds import process_cripto_spreads
from services.data_lake.nse_spreads import process_nse_spreads
from services.data_lake.crypto_ws import ws_runner
from services.algo_signals.signal import process_symbol_signal
from services.data_lake.binance import Binance_Symbol_gap_filler
from services.broker_auth.main import Fyers_Auth
from services.loger import logger
from symbol_list import BINANCE_SYMBOLS, FYERS_SYMBOLS, DB_SYMBOLS, NSE_SYMBOLS, ETF_SYMBOLS,SNP_SYMBOLS
import sys
from services.config import redis_client
from concurrent.futures import ThreadPoolExecutor

# Command-line arguments
parser = argparse.ArgumentParser(description='Pipeline Running.')
parser.add_argument('--exchange', required=True, choices=['binance','snp','etf', 'nse', 'backtest_nse','backtest_binance','backtest_snp','backtest_etf'])
args = parser.parse_args()
EXCHANGE = args.exchange.lower()

# EXCHANGE = "binance"

parts = EXCHANGE.split("_")
if len(parts) >= 2 and parts[0] == 'backtest':
    EXCHANGE = parts[1]
    backtest = parts[0]
else:
    EXCHANGE = EXCHANGE
    backtest = None

def auth_process():
        if EXCHANGE == 'nse':
            Fyers_Auth()

def symbol_gap_filler():
    if EXCHANGE == 'binance':
        for symbol in BINANCE_SYMBOLS:
            Binance_Symbol_gap_filler(symbol)

    elif EXCHANGE == 'nse':
        for full_symbol in FYERS_SYMBOLS:
            symbol = full_symbol.split(":")[1].split("-")[0]
            fyers_Symbol_gap_filler(symbol)

def symbol_1m_filler():
    if EXCHANGE == 'binance':
        with Pool(processes=10) as pool:
            pool.map(Binance_Symbol_gap_filler, BINANCE_SYMBOLS)

    elif EXCHANGE == 'nse':
        symbols = [full_symbol.split(":")[1].split("-")[0] for full_symbol in FYERS_SYMBOLS]
        with Pool(processes=10) as pool:
            pool.map(fyers_Symbol_gap_filler, symbols)

def save_spreads_list():
    mapping = {
        "binance": ("spreads:binance_spreads_name", "manual_symbols:user_pairs_binance"),
        "nse":     ("spreads:nse_spreads_name", "manual_symbols:user_pairs_nse"),
        "snp":     ("spreads:snp_spreads_name", "manual_symbols:user_pairs_snp"),
        "etf":     ("spreads:etf_spreads_name", "manual_symbols:user_pairs_etf"),
    }

    if EXCHANGE not in mapping:
        raise ValueError(f"Unsupported EXCHANGE value: {EXCHANGE}")

    redis_key, input_key = mapping[EXCHANGE]
    manual_pairs = json.loads(redis_client.get(input_key) or "[]")
    new_pairs = {f"{a.lower()}_{b.lower()}" for a, b in manual_pairs}

    existing = {x.decode() for x in redis_client.smembers(redis_key) or set()}
    to_add = new_pairs - existing

    if to_add:
        redis_client.sadd(redis_key, *to_add)
        redis_client.persist(redis_key)

    updated_pairs = [x.decode() for x in redis_client.smembers(redis_key)]

def Spreads_gap_filler(exchange):
    print(f"Spreads gap filler start {datetime.now()}")
    redis_key = f'manual_symbols:user_pairs_{exchange}'
    symbol_pairs = json.loads(redis_client.get(redis_key))
    with ThreadPoolExecutor(max_workers=10) as pool:
        if exchange == 'nse':
            pool.map(lambda pair: process_nse_spreads(pair[0].upper(), pair[1].upper()), symbol_pairs)
        else:
            pool.map(lambda pair: process_cripto_spreads(pair[0].upper(), pair[1].upper()), symbol_pairs)
    print("Spreads gap filler completed")

def signal_proces():
    try:
        redis_key = f'spreads:{EXCHANGE}_spreads_name'
        symbol_pairs = [p.decode() for p in redis_client.smembers(redis_key)]

        with ThreadPoolExecutor(max_workers=10) as pool:
            pool.map(lambda pair: process_symbol_signal(pair, EXCHANGE), symbol_pairs)
    except Exception as e:
        logger.error(f"Error in signal_proces: {e}")


import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def view_refresh():
    TRADING_SYSTEM_CONN_PARAMS = {
        "dbname": "trading_system",
        "user": "postgres",
        "password": "onealpha12345",
        "host": "localhost",
        "port": 5432
    }

    tables = ["binance_stocks", "binance_spreads","nse_stocks", "nse_spreads"]
    intervals = ["1m", "5m", "15m", "30m", "1d"]

    conn = psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    for table in tables:
        for interval in intervals:
            view_name = f"{table}_{interval}"
            try:
                cur.execute(f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL);")
            except Exception as e:
                print(f"Error refreshing view {view_name}: {e}")

    cur.close()
    conn.close()
    redis_client.hset("ui_alert", "data_pull", json.dumps({"data_pull": True}))
    print("✅ All continuous aggregates refreshed.")

def run_gap_fillers_loop():
    while True:
        now = datetime.now()
        # Calculate next run time at next minute + 5 seconds
        next_run = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        wait_time = (next_run - now).total_seconds()
        print(f"\n[{now}] Sleeping {wait_time:.2f} seconds to sync with next run at {next_run}...")
        time.sleep(wait_time)

        print(f"[{datetime.now()}] Starting cycle...")
        symbol_1m_filler()
        Spreads_gap_filler(EXCHANGE)
        # view_refresh()
        threading.Thread(target=view_refresh, daemon=True, name="view_refresh").start()
        signal_proces()

if __name__ == "__main__":
    if backtest == 'backtest':
        pass
    ########---------Backtest module end-------############

    # Run authentication once (only for Fyers)
    if EXCHANGE == 'nse':
        auth_process()

    symbol_gap_filler()
    print('Symbol_gap_filler completed')

    Spreads_gap_filler(EXCHANGE)

    # view_refresh()
    threading.Thread(target=view_refresh, daemon=True, name="view_refresh").start()
    
    threading.Thread(target=ws_runner, daemon=True, name="WebSocketRunnerThread").start()

    # Run signal processing in a separate thread
    signal_proces()
    print('signal_proces completed')
    
    # Run every minute gap filler
    threading.Thread(target=run_gap_fillers_loop, daemon=True, name="GapFillerLoopThread").start()

    # Main thread ko alive rakhne ke liye
    try:
        while True:
            time.sleep(60)  # Main thread alive rakho
            print(f"Main thread alive at {datetime.now()}")
    except KeyboardInterrupt:
        print("Shutting down...")
        sys.exit(0)