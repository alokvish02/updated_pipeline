import json
import asyncio
import websockets
import threading
from datetime import datetime
import pytz
from collections import deque
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
from services.config import redis_connection

redis_client = redis_connection()
ACCOUNT_KEY = "account_matrix:account"
window_raw = redis_client.hget(ACCOUNT_KEY, "window")
window = int(window_raw) if window_raw is not None else None

# Fetch pair list from Redis set
SPREADS_SET_KEY = "spreads:binance_spreads_name"
pair_list = list(redis_client.smembers(SPREADS_SET_KEY))
pair_list = [pair.decode("utf-8") for pair in pair_list]

IST = pytz.timezone("Asia/Kolkata")
WINDOW_SIZE = window
EXCHANGE = "binance"
PAIRS = {pair: [pair.split('_')[0], pair.split('_')[1]] for pair in pair_list}
UNIQUE_SYMBOLS = list(set([sym for pair in PAIRS.values() for sym in pair]))
URI = "wss://stream.binance.com:9443/ws"

# Initialize data structures
real_time_data = {sym: deque(maxlen=WINDOW_SIZE) for sym in UNIQUE_SYMBOLS}
data_lock = threading.Lock()
latest_prices = {}
historical_slopes = {pair: None for pair in pair_list}

SPREAD_DATA_KEY = "spreads:live_data"
LTP_DATA_KEY = "binance_ltp:stocks"

def _fetch_latest_slope_from_db(symbol: str) -> float:
    """Fetch latest slope from database"""
    symbol = symbol.lower()
    conn_params = {
        "dbname": "trading_system",
        "user": "postgres", 
        "password": "onealpha12345",
        "host": "localhost",
        "port": 5432
    }
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            query = """
                SELECT slope
                FROM public.binance_spreads
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """
            cur.execute(query, (symbol,))
            row = cur.fetchone()
            if row:
                return float(row[0])
            return None

def calculate_live_spread(price1: float, price2: float, slope: float) -> dict:
    """Calculate live spread using current prices and historical slope"""
    current_time = datetime.now(IST)
    live_spread = price1 - (slope * price2)

    return {
        "timestamp": current_time,
        "close": live_spread,
        "slope": slope
    }

async def binance_ws_handler():
    global historical_slopes, latest_prices
    
    # Timer for fetching slope from DB every minute
    last_db_fetch = datetime.now()

    # Initialize slopes from database
    for pair in pair_list:
        slope = _fetch_latest_slope_from_db(pair)
        if slope is not None:
            historical_slopes[pair] = slope
            print(f"[INIT] {pair}: slope={slope:.6f}")

    async with websockets.connect(URI) as websocket:
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": [f"{s}@trade" for s in UNIQUE_SYMBOLS],
            "id": 1
        }
        await websocket.send(json.dumps(subscribe_msg))
        print(f"[WS] Subscribed to {len(UNIQUE_SYMBOLS)} symbols")

        while True:
            message = await websocket.recv()
            data = json.loads(message)

            # Update slopes from DB every minute
            current_time = datetime.now()
            if (current_time - last_db_fetch).seconds >= 60:
                for pair in pair_list:
                    slope = _fetch_latest_slope_from_db(pair)
                    if slope is not None:
                        historical_slopes[pair] = slope
                last_db_fetch = current_time
                print("last_db_fetch", last_db_fetch)
                print("[DB] Updated slopes from database")

            if 'e' in data and data['e'] == 'trade' and 's' in data and 'p' in data:
                symbol = data['s'].lower()
                price = float(data['p'])

                with data_lock:
                    if symbol in real_time_data:
                        latest_prices[symbol] = price
                        # Store LTP in Redis
                        ltp_data = {
                            "symbol": symbol,
                            "price": str(price),
                            "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
                        }
                        redis_client.hset(LTP_DATA_KEY, symbol, json.dumps(ltp_data))

                # Calculate live spread for relevant pairs
                for pair, symbols in PAIRS.items():
                    sym1, sym2 = symbols
                    if (symbol in [sym1, sym2] and
                            historical_slopes[pair] is not None and
                            sym1 in latest_prices and
                            sym2 in latest_prices):
                        
                        live_spread = calculate_live_spread(
                            latest_prices[sym1],
                            latest_prices[sym2],
                            historical_slopes[pair]
                        )

                        spread_data = {
                            "close": str(live_spread["close"]),
                            "slope": str(live_spread["slope"]),
                            "timestamp": live_spread["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                            "symbol1": sym1,
                            "symbol2": sym2
                        }
                        # print(spread_data)
                        redis_client.hset(SPREAD_DATA_KEY, pair, json.dumps(spread_data))

def ws_runner():
    asyncio.run(binance_ws_handler())

if __name__ == "__main__":
    ws_runner()