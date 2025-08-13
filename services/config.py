import os
import redis
import time

# Configure Redis parameters from environment variables (with defaults)
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REALTIME_CACHE_TTL = int(os.getenv("REALTIME_CACHE_TTL", 20))  # TTL for real-time data

# Initialize Redis client with connection pooling and timeout settings
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    socket_timeout=5,  # seconds
    socket_connect_timeout=5
)

redis_key = f"account_matrix:account"
account_matrix = redis_client.hgetall(redis_key)
account_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in account_matrix.items()}

lookback = int(account_data.get("lookback"))
std = int(account_data.get("std"))
strategy_name = account_data.get("strategy_name")

config = {
    'params': {
        'strategy': strategy_name, 
        'window': lookback,
        'std': std
    },
    # Redis keys
    'redis_keys': {
        'spreads_live_data': 'spreads:live_data',
        'account_matrix': 'account_matrix:account'
    },
      
    # Cache parameters
    'cache_params': {
        'trade_check_ttl': 300
    },
}


def redis_connection(retries=5, delay=2):
    """
    Attempts to ping Redis until a connection is established.
    :param retries: Number of retry attempts.
    :param delay: Delay in seconds between retries.
    :return: A connected Redis client.
    :raises Exception: If unable to connect after all retries.
    """
    attempt = 0
    while attempt < retries:
        try:
            if redis_client.ping():
                return redis_client
        except Exception as e:
            print(f"Error connecting to Redis on attempt {attempt + 1}: {e}")
        attempt += 1
        time.sleep(delay)

    raise Exception("Unable to connect to Redis after multiple attempts.")

