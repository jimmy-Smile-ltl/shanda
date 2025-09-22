import random
import time
import json
import redis
import threading

class RedisTokenBucket:
    LUA_SCRIPT = """
    local key = KEYS[1]
    local rate = tonumber(ARGV[1])
    local capacity = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])
    local need = tonumber(ARGV[4])

    local data = redis.call("HMGET", key, "tokens", "timestamp")
    local tokens = tonumber(data[1])
    local timestamp = tonumber(data[2])

    if tokens == nil then
        tokens = capacity
        timestamp = now
    end

    local delta = math.max(0, now - timestamp)
    tokens = math.min(capacity, tokens + delta * rate)
    local allowed = tokens >= need

    if allowed then
        tokens = tokens - need
    end

    redis.call("HMSET", key, "tokens", tokens, "timestamp", now)
    return allowed
    """

    def __init__(self, redis_client, key="project:token_bucket", rate=5, capacity=10):
        self.redis = redis_client
        self.key = key
        self.rate = rate
        self.capacity = capacity
        self._script = self.redis.register_script(self.LUA_SCRIPT)

    def try_acquire(self, tokens=1):
        now = int(time.time())
        return bool(self._script(keys=[self.key], args=[self.rate, self.capacity, now, tokens]))

    def acquire(self, tokens=1, interval=0.1):
        while True:
            if self.try_acquire(tokens):
                return True
            time.sleep(interval)


class ProxyManager:
    """
    单例模式 ProxyManager，支持 Redis 存储字典代理 + 阻塞/非阻塞限流
    """
    _instance_lock = threading.Lock()
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, redis_client, limiter: RedisTokenBucket, redis_key="proxy:list"):
        if hasattr(self, "_initialized") and self._initialized:
            return
        self.redis = redis_client
        self.limiter = limiter
        self.redis_key = redis_key
        self._initialized = True

    def _get_all_proxies(self):
        proxies = self.redis.lrange(self.redis_key, 0, -1)
        return [json.loads(p) for p in proxies]

    def get_proxy(self):
        """阻塞模式获取代理"""
        self.limiter.acquire()
        proxies = self._get_all_proxies()
        return random.choice(proxies) if proxies else None

    def try_get_proxy(self):
        """非阻塞模式获取代理"""
        if self.limiter.try_acquire():
            proxies = self._get_all_proxies()
            return random.choice(proxies) if proxies else None
        return None


# 全局初始化函数，保证单例
_global_proxy_manager = None
_global_lock = threading.Lock()

def get_proxy_manager(redis_host="192.168.130.53", redis_port=6379, redis_password=None,
                      rate=5, capacity=5, redis_key="proxy:list"):
    global _global_proxy_manager
    if _global_proxy_manager is None:
        with _global_lock:
            if _global_proxy_manager is None:
                r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, password=redis_password)
                limiter = RedisTokenBucket(r, key=f"{redis_key}:bucket", rate=rate, capacity=capacity)
                _global_proxy_manager = ProxyManager(r, limiter, redis_key=redis_key)
    return _global_proxy_manager


if __name__=="__main__":
    proxy_manager = get_proxy_manager(
        redis_host="192.168.130.53",
        redis_port=6379,
        redis_password="123456",
        rate=5,
        capacity=5,
        redis_key="proxy:list"
    )
    proxies = proxy_manager.get_proxy()
    print(proxies)
