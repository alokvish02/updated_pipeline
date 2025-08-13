import redis
from services.config import redis_connection
from threading import Lock

class FilterManager:
    def __init__(self):
        self.redis = redis_connection()
        self.key = "global_filter"
        self._lock = Lock()

        if not self.redis.exists(self.key):
            self.set_filter("1w", "", 0, 100)

    def set_filter(self, period, exchange, offset, limit):
        with self._lock:
            self.redis.hset(self.key, "period", period)
            self.redis.hset(self.key, "exchange", exchange)
            self.redis.hset(self.key, "offset", offset)
            self.redis.hset(self.key, "limit", limit)

    def get_filter(self):
        with self._lock:
            result = self.redis.hgetall(self.key)
            return {
                "period": result.get(b"period", b"1w").decode("utf-8"),
                "exchange": result.get(b"exchange", b"").decode("utf-8"),
                "offset": int(result.get(b"offset", b"0")),
                "limit": int(result.get(b"limit", b"100"))
            }

# Singleton instance
filter_manager = FilterManager()
