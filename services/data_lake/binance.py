import time
import pandas as pd
from datetime import datetime, timedelta
import requests
import pytz
from services.config import redis_connection
import json
from psycopg2.extras import execute_batch
import psycopg2
IST = pytz.timezone("Asia/Kolkata")
redis_client = redis_connection()
redis_key = f"account_matrix:account"

# Fetch hash from Redis and decode keys/values
account_matrix = redis_client.hgetall(redis_key)
account_matrix = {k.decode('utf-8'): v.decode('utf-8') for k, v in account_matrix.items()}

# Correct: don't add commas here
start_date = account_matrix["from_date"]
end_date = account_matrix["to_date"]

# Convert to datetime
from datetime import datetime

start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

# Calculate lookback
DEFAULT_LOOKBACK_DAYS = (end_date_dt - start_date_dt).days

#now this is checking last row from the database
def get_last_cached_timestamp(prefix, symbol):
    query = """
        SELECT MAX(timestamp)
        FROM public.binance_stocks
        WHERE symbol = %s;
    """
    with psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (symbol,))
            result = cur.fetchone()
            if result and result[0]:
                return result[0].astimezone(IST)
            return None

TRADING_SYSTEM_CONN_PARAMS = {
    "dbname": "trading_system",
    "user": "postgres",
    "password": "onealpha12345",
    "host": "localhost",
    "port": 5432
}

def cache_data_binance(df, symbol):
    if df is None or df.empty:
        print(f"No new data to insert for {symbol}.")
        return

    try:
        # Get current time in IST
        current_time = datetime.now(IST)
        # Calculate the end of the last complete minute
        last_complete_minute = current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)

        # Filter out the current candle (incomplete candle)
        df = df[df['timestamp'] <= last_complete_minute]

        if df.empty:
            print(f"No complete candle data to insert for {symbol} after filtering.")
            return

        # Prepare data for insertion
        rows = []
        for record in df.to_dict(orient="records"):
            ts = record["timestamp"]
            if isinstance(ts, pd.Timestamp):
                ts = ts.to_pydatetime()
            ts = ts.astimezone(IST)
            rows.append((
                symbol,
                ts,
                record["open"],
                record["high"],
                record["low"],
                record["close"],
                record["volume"]
            ))

        # Insert data into the database
        with psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS) as conn:
            with conn.cursor() as cur:
                upsert_query = """
                    INSERT INTO public.binance_stocks (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """
                execute_batch(cur, upsert_query, rows)
            conn.commit()
            print(f"✅ Upserted {len(rows)} rows into 'binance_stocks' for {symbol}.")

    except Exception as e:
        print(f"⚠️ Error upserting data for {symbol}: {str(e)}")

def fetch_data(symbol, start_time, end_time):
    all_data = []
    current_start = start_time
    max_retries = 5
    base_delay = 5

    while current_start < end_time:
        current_end = min(current_start + timedelta(minutes=1000), end_time)
        start_ms = int(current_start.timestamp() * 1000)
        end_ms = int(current_end.timestamp() * 1000)
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&startTime={start_ms}&endTime={end_ms}"

        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()

                # Rate limit handling
                if 'X-MBX-USED-WEIGHT-1M' in response.headers:
                    used_weight = int(response.headers['X-MBX-USED-WEIGHT-1M'])
                    if used_weight > 1000:
                        sleep_time = min(base_delay * (2 ** attempt), 60)
                        print(f"Rate limit ({used_weight}) for {symbol}, waiting {sleep_time}s")
                        time.sleep(sleep_time)
                        continue

                data = response.json()
                if not data:
                    print(f" No data for {symbol} from {current_start} to {current_end}")
                    break

                # Process candles and convert timestamps from UTC to IST (GMT+5:30)
                for candle in data:
                    ts = datetime.fromtimestamp(candle[0] / 1000, tz=pytz.UTC).astimezone(IST)
                    all_data.append({
                        "symbol": symbol,
                        "timestamp": ts,
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5])
                    })

                last_ts = datetime.fromtimestamp(data[-1][0] / 1000, tz=pytz.UTC).astimezone(IST)
                current_start = last_ts + timedelta(minutes=1)
                time.sleep(0.2)  # Throttle requests
                break
            except requests.exceptions.RequestException as e:
                sleep_time = base_delay * (2 ** attempt)
                print(f" Attempt {attempt + 1} failed for {symbol}: {e} - waiting {sleep_time}s")
                time.sleep(sleep_time)
                if attempt == max_retries - 1:
                    print(f" Failed final attempt for {symbol}")
                    return pd.DataFrame(all_data)
    return pd.DataFrame(all_data)


def Binance_Symbol_gap_filler(symbol):
    # print(f"Filling missing data for {symbol}...")
    prefix = "binance"
    last_ts = get_last_cached_timestamp(prefix, symbol)
    current_time = datetime.now(IST)

    if last_ts is None:
        # Initial data load
        start_time = current_time - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        df = fetch_data(symbol, start_time, current_time)
        cache_data_binance(df, symbol)
    else:
        gap_start = last_ts + timedelta(minutes=1)
        if gap_start > current_time:
            return  # No gap to fill

        # Fill missing data directly via API
        df = fetch_data(symbol, gap_start, current_time)
        cache_data_binance(df, symbol)
