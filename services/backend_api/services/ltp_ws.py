import json
from datetime import datetime
from dateutil.parser import parse as parse_datetime

from services.config import redis_client
def normalize_symbol(symbol: str) -> str:
    """Convert symbols like BINANCE_AVAXUSDT_BNBUSDT to avaxusdt_bnbusdt"""
    if symbol.upper().startswith("BINANCE_"):
        parts = symbol.replace("BINANCE_", "").split("_")
        if len(parts) == 2:
            return f"{parts[0].lower()}_{parts[1].lower()}"
    return symbol.lower()

def format_timestamp(timestamp):
    """
    Return the timestamp as epoch (int seconds since 1970-01-01 UTC).
    """
    try:
        if isinstance(timestamp, datetime):
            return int(timestamp.timestamp())
        dt = parse_datetime(str(timestamp))
        return int(dt.timestamp())
    except Exception as e:
        print(f"Error formatting timestamp {timestamp}: {e}")
        return 0


def fetch_ltp(symbol, redis_client):
    normalized_symbol = normalize_symbol(symbol)
    try:
        live_data = redis_client.hget('spreads:live_data', normalized_symbol)

        if live_data:
            live_data = json.loads(live_data)
            return {
                'symbol': symbol,
                'ltp': float(live_data.get('close')),
                'timestamp': format_timestamp(live_data.get('timestamp'))
            }
        else:
            return {
                'symbol': symbol,
                'error': 'No live data available'
            }

    except Exception as e:
        # print(f"Error fetching LTP for symbol {symbol}: {e}")
        return None

# data = fetch_ltp("BINANCE_AVAXUSDT_BNBUSDT", redis_client)
# print(data)