import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta
from services.data_lake.spreads_helper import SpreadCalculator
from services.config import redis_connection
from psycopg2.extras import execute_values

redis_client = redis_connection()

def get_db_connection():
    return psycopg2.connect(dbname="trading_system", user="postgres", password="onealpha12345", host="localhost", port=5432)

def now_ist():
    return datetime.now().replace(second=0, microsecond=0)

def get_cached_ohlc_data(symbol, start=None, end=None):
    symbol = symbol.upper()
    start_time = start or datetime(2016, 1, 1)
    end_time = end or now_ist()
    query = """
        SELECT symbol, timestamp, open, high, low, close, volume
        FROM public.nse_stocks
        WHERE symbol = %s AND timestamp >= %s AND timestamp < %s
        ORDER BY timestamp ASC
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (symbol, start_time, end_time))
        df = pd.DataFrame(cur.fetchall(), columns=['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume'])
    if df.empty:
        return df
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.sort_values("timestamp")

def get_last_spread_timestamp(pair_name):
    query = """
        SELECT timestamp FROM public.nse_spreads
        WHERE symbol = %s ORDER BY timestamp DESC LIMIT 1
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (pair_name,))
        result = cur.fetchone()
    return result[0] if result else None

def subtract_nse_minutes(start_datetime, minutes):
    current = start_datetime
    remaining_minutes = minutes
    while remaining_minutes > 0:
        # Check if current time is within NSE market hours (Monday to Friday, 9:15 AM to 3:30 PM)
        is_weekday = current.weekday() < 5  # Monday=0, Sunday=6
        is_market_hours = current.time() >= datetime.strptime("09:15", "%H:%M").time() and current.time() <= datetime.strptime("15:30", "%H:%M").time()
        
        if is_weekday and is_market_hours:
            remaining_minutes -= 1
        current -= timedelta(minutes=1)
        
        # Skip to previous market day if outside market hours
        if not is_weekday or current.time() < datetime.strptime("09:15", "%H:%M").time():
            current = current.replace(hour=15, minute=30, second=0, microsecond=0)
            if current.weekday() >= 5:  # If Saturday/Sunday, move to Friday
                current -= timedelta(days=current.weekday() - 4)
            else:
                current -= timedelta(days=1)
    return current

def ensure_window_data_availability(sym1, sym2, pair_name, window):
    last_spread = get_last_spread_timestamp(pair_name)
    if last_spread is None:
        return datetime(2023, 1, 1)
    lookback_start = subtract_nse_minutes(last_spread, window)
    df1 = get_cached_ohlc_data(sym1, start=lookback_start, end=last_spread + timedelta(minutes=1))
    df2 = get_cached_ohlc_data(sym2, start=lookback_start, end=last_spread + timedelta(minutes=1))
    min_available = min(len(df1) if not df1.empty else 0, len(df2) if not df2.empty else 0)
    if min_available < window:
        return subtract_nse_minutes(lookback_start, window - min_available + 10)
    return lookback_start

def insert_spread_data_to_db(spread_df, pair_name):
    print(f"Inserting spread data for {pair_name} with {len(spread_df)} records")
    if spread_df.empty:
        return
        
    spread_df['symbol'] = pair_name.lower()
    
    insert_query = """
        INSERT INTO public.nse_spreads (symbol, timestamp, open, high, low, close, volume, slope)
        VALUES %s ON CONFLICT (symbol, timestamp) DO NOTHING
    """
    data_tuples = spread_df[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'slope']].values.tolist()
    
    with get_db_connection() as conn, conn.cursor() as cur:
        execute_values(cur, insert_query, data_tuples)
        conn.commit()

def delete_spreads_name():
    redis_client.delete('spreads:nse_spreads_name')

def save_spread_symbol_to_db(spread_symbols):
    if spread_symbols:
        cleaned = np.array([symbol.replace(':', '').lower() for symbol in ([spread_symbols] if isinstance(spread_symbols, str) else spread_symbols)])
        redis_client.sadd('spreads:nse_spreads_name', *cleaned)

def fill_historical_gaps(pair_name):
    calculator = SpreadCalculator("nse")
    sym1, sym2 = pair_name.split("_")
    window = int(redis_client.hget("account_matrix:account", "window"))
    lookback_start = ensure_window_data_availability(sym1, sym2, pair_name, window + 5000)
    last_spread = get_last_spread_timestamp(pair_name)
    new_start = last_spread + timedelta(minutes=1) if last_spread else lookback_start
    df1 = get_cached_ohlc_data(sym1, start=lookback_start)
    df2 = get_cached_ohlc_data(sym2, start=lookback_start)
    if df1.empty or df2.empty:
        return
    spread_data = calculator.calculate_historical_spread(df1, df2, window)
    spread_data = spread_data.iloc[2:] # Skip the first row to useless data
    # print(f"Historical gaps filled for {pair_name} with {len(spread_data)} records")
    print(spread_data.tail())
    if not spread_data.empty:
        if last_spread:
            spread_data = spread_data[spread_data['timestamp'] > last_spread]
        if not spread_data.empty:
            pass
            insert_spread_data_to_db(spread_data, pair_name)

def process_nse_spreads(sym1, sym2):
    print(f"Processing NSE spreads for {sym1} and {sym2}")
    calculator = SpreadCalculator("nse")
    pair_name = calculator.generate_pair_name(sym1, sym2)
    save_spread_symbol_to_db(pair_name)
    fill_historical_gaps(pair_name)
    return True