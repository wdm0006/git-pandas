import pickle

try:
    import redis

    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False


def multicache(key_prefix, key_list, skip_if=None):
    def multicache_nest(func):
        def deco(self, *args, **kwargs):
            if self.cache_backend is None:
                return func(self, *args, **kwargs)
            else:
                if skip_if is not None and skip_if(kwargs):
                    return func(self, *args, **kwargs)

                key = key_prefix + self.repo_name + "_".join([str(kwargs.get(k)) for k in key_list])
                try:
                    if isinstance(self.cache_backend, EphemeralCache | RedisDFCache):
                        ret = self.cache_backend.get(key)
                        return ret
                    else:
                        raise ValueError("Unknown cache backend type")
                except CacheMissError:
                    ret = func(self, *args, **kwargs)
                    self.cache_backend.set(key, ret)
                    return ret

        return deco

    return multicache_nest


class CacheMissError(Exception):
    pass


class EphemeralCache:
    """
    A simple in-memory cache.
    """

    def __init__(self, max_keys=1000):
        self._cache = {}
        self._key_list = []
        self._max_keys = max_keys

    def evict(self, n=1):
        for _ in range(n):
            key = self._key_list.pop(0)
            del self._cache[key]

    def set(self, k, v):
        try:
            idx = self._key_list.index(k)
            self._key_list.pop(idx)
            self._key_list.append(k)
        except ValueError:
            self._key_list.append(k)

        self._cache[k] = v

        if len(self._key_list) > self._max_keys:
            self.evict(len(self._key_list) - self._max_keys)

    def get(self, k):
        if self.exists(k):
            # Move the key to the end of the list (most recently used)
            idx = self._key_list.index(k)
            self._key_list.pop(idx)
            self._key_list.append(k)
            return self._cache[k]
        else:
            raise CacheMissError(k)

    def exists(self, k):
        return k in self._cache


class RedisDFCache:
    """
    A redis based cache, using redis-py under the hood.

    :param host: default localhost
    :param port: default 6379
    :param db: the database to use, default 12
    :param max_keys: the max number of keys to cache, default 1000
    :param ttl: time to live for any cached results, default None
    :param kwargs: additional options available to redis.StrictRedis
    """

    def __init__(self, host="localhost", port=6379, db=12, max_keys=1000, ttl=None, **kwargs):
        if not _HAS_REDIS:
            raise ImportError("Need redis installed to use redis cache")
        self._cache = redis.StrictRedis(host=host, port=port, db=db, **kwargs)
        self._key_list = []
        self._max_keys = max_keys
        self.ttl = ttl
        self.prefix = "gitpandas_"

        # sync with any keys that already exist in this database (order will not be preserved)
        self.sync()

    def evict(self, n=1):
        for _ in range(n):
            key = self._key_list.pop(0)
            self._cache.delete(key)

    def set(self, orik, v):
        k = self.prefix + orik
        try:
            idx = self._key_list.index(k)
            self._key_list.pop(idx)
            self._key_list.append(k)
        except ValueError:
            self._key_list.append(k)

        # Use pickle instead of msgpack for DataFrame serialization
        self._cache.set(k, pickle.dumps(v), ex=self.ttl)

        if len(self._key_list) > self._max_keys:
            self.evict(len(self._key_list) - self._max_keys)

    def get(self, orik):
        k = self.prefix + orik
        if self.exists(orik):
            # Move the key to the end of the list (most recently used)
            idx = self._key_list.index(k)
            self._key_list.pop(idx)
            self._key_list.append(k)
            # Use pickle instead of msgpack for DataFrame deserialization
            return pickle.loads(self._cache.get(k))
        else:
            try:
                idx = self._key_list.index(k)
                self._key_list.pop(idx)
            except ValueError:
                pass
            raise CacheMissError(k)

    def exists(self, k):
        k = self.prefix + k
        return self._cache.exists(k)

    def sync(self):
        """
        Syncs the key list with what is in redis.
        :return: None
        """
        self._key_list = [x.decode("utf-8") for x in self._cache.keys(self.prefix + "*")]

    def purge(self):
        for key in self._cache.scan_iter(f"{self.prefix}*"):
            self._cache.delete(key)
