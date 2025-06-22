import gzip
import logging
import os
import pickle
import threading

try:
    import redis

    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False


def multicache(key_prefix, key_list, skip_if=None):
    """Decorator to cache the results of a method call.

    Args:
        key_prefix (str): Prefix for the cache key.
        key_list (list[str]): List of argument names (from kwargs) to include in the cache key.
        skip_if (callable, optional): A function that takes kwargs and returns True
            if caching should be skipped entirely (no read, no write). Defaults to None.

    The decorated method can accept an optional `force_refresh=True` argument
    to bypass the cache read but still update the cache with the new result.
    This force_refresh state propagates to nested calls on the same object instance.
    """

    def multicache_nest(func):
        def deco(self, *args, **kwargs):
            # If no cache backend, just run the function
            if self.cache_backend is None:
                return func(self, *args, **kwargs)

            # Check if caching should be skipped entirely based on skip_if
            if skip_if is not None and skip_if(kwargs):
                logging.debug(f"Cache skipped entirely for {key_prefix} due to skip_if condition.")
                return func(self, *args, **kwargs)

            # Check for propagation flag first, then explicit force_refresh argument
            is_propagated_force = getattr(self, "_is_forcing_refresh", False)
            explicit_force_refresh = kwargs.pop("force_refresh", False)
            force_refresh = is_propagated_force or explicit_force_refresh

            # Generate the cache key (ensure force_refresh itself is not part of the key)
            key_parts = [str(kwargs.get(k)) for k in key_list]
            key = f"{key_prefix}_{self.repo_name}_{'_'.join(key_parts)}"
            logging.debug(f"Cache key generated for {key_prefix}: {key}")

            # Explicitly log force refresh bypass of cache read
            if force_refresh:
                logging.info(
                    f"Force refresh active (propagated: {is_propagated_force}, explicit: {explicit_force_refresh}) "
                    f"for key: {key}, bypassing cache read."
                )

            cache_hit = False
            # Try retrieving from cache if not forcing refresh
            if not force_refresh:
                try:
                    # Include DiskCache in the type check
                    if isinstance(self.cache_backend, EphemeralCache | RedisDFCache | DiskCache):
                        ret = self.cache_backend.get(key)
                        logging.debug(f"Cache hit for key: {key}")
                        cache_hit = True
                        return ret
                    else:
                        logging.warning(f"Cannot get from cache; unknown backend type: {type(self.cache_backend)}")

                except CacheMissError:
                    logging.debug(f"Cache miss for key: {key}")
                    pass  # Proceed to execute the function
                except Exception as e:
                    logging.error(f"Error getting cache for key {key}: {e}", exc_info=True)

            # Execute the function if cache missed, force_refresh is True, or cache read failed
            if not cache_hit:
                # Set the temporary flag *before* calling the function if we are forcing refresh
                set_force_flag = force_refresh and not is_propagated_force
                if set_force_flag:
                    self._is_forcing_refresh = True
                    logging.debug(f"Setting _is_forcing_refresh flag for nested calls originating from {key_prefix}")

                try:
                    logging.debug(f"Executing function for key: {key} (Force Refresh: {force_refresh})")
                    ret = func(self, *args, **kwargs)

                    # Store the result in the cache (always happens after function execution)
                    try:
                        if isinstance(self.cache_backend, EphemeralCache | RedisDFCache | DiskCache):
                            self.cache_backend.set(key, ret)
                            logging.debug(f"Cache set for key: {key}")
                        else:
                            logging.warning(f"Cannot set cache; unknown backend type: {type(self.cache_backend)}")
                    except Exception as e:
                        logging.error(f"Failed to set cache for key {key}: {e}", exc_info=True)

                    return ret
                finally:
                    # Always remove the flag after the function call completes or errors
                    if set_force_flag:
                        delattr(self, "_is_forcing_refresh")
                        logging.debug(f"Removed _is_forcing_refresh flag after call to {key_prefix}")

            # This should only be reached if cache_hit was True and force_refresh was False
            # The earlier return inside the cache hit block handles this case.
            # Adding an explicit return here for clarity, although theoretically unreachable.
            return ret  # Should have already returned if cache_hit was True

        return deco

    return multicache_nest


class CacheMissError(Exception):
    pass


class DataFrameEncodingError(Exception):
    """Custom exception for errors during DataFrame encoding/decoding."""

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

        # Add empty save method call for compatibility with DiskCache
        if hasattr(self, "save"):
            self.save()  # Only call save if it exists to maintain backward compatibility

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

    # Add empty save method for compatibility with DiskCache
    def save(self):
        """Empty save method for compatibility with DiskCache."""
        pass


class DiskCache(EphemeralCache):
    """
    An in-memory cache that can be persisted to disk using pickle.

    Inherits LRU eviction logic from EphemeralCache.
    Thread-safe for concurrent access.
    """

    def __init__(self, filepath, max_keys=1000):
        """
        Initializes the cache. Tries to load from the specified filepath
        if it exists.

        :param filepath: Path to the file for persisting the cache.
        :param max_keys: Maximum number of keys to keep in the cache (LRU).
        """
        super().__init__(max_keys=max_keys)
        self.filepath = filepath
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self.load()  # Attempt to load existing cache on initialization

    def set(self, k, v):
        """
        Thread-safe set operation that prevents nested save calls.
        """
        with self._lock:
            # Temporarily disable automatic save during parent set() to prevent nested calls
            original_save = getattr(self, 'save', None)
            self.save = lambda: None  # Temporarily disable save
            try:
                # Call parent set without triggering save
                super().set(k, v)
            finally:
                # Restore original save method
                if original_save:
                    self.save = original_save
                # Now save once, safely under lock
                self.save()

    def get(self, k):
        """
        Thread-safe get operation with disk loading capability.
        """
        with self._lock:
            # Initial check in memory
            if k in self._cache:
                # Move the key to the end of the list (most recently used)
                try:
                    idx = self._key_list.index(k)
                    self._key_list.pop(idx)
                    self._key_list.append(k)
                except ValueError:
                    # Should not happen if k is in _cache, but handle defensively
                    # If key is in cache but not list, add it to the list (end)
                    self._key_list.append(k)
                    # If list exceeds max keys due to this, handle it (though ideally cache and list are always synced)
                    if len(self._key_list) > self._max_keys:
                        self.evict(len(self._key_list) - self._max_keys)
                return self._cache[k]
            else:
                # Key not in memory, try loading from disk
                logging.debug(f"Key '{k}' not in memory cache, attempting disk load.")
                self.load()
                # Check again after loading
                if k in self._cache:
                    logging.debug(f"Key '{k}' found in cache after disk load.")
                    # Update LRU list as it was just accessed
                    try:
                        idx = self._key_list.index(k)
                        self._key_list.pop(idx)
                        self._key_list.append(k)
                    except ValueError:
                        # Add to list if somehow missing after load
                        self._key_list.append(k)
                        if len(self._key_list) > self._max_keys:
                            self.evict(len(self._key_list) - self._max_keys)
                    return self._cache[k]
                else:
                    # Key not found even after loading from disk
                    logging.debug(f"Key '{k}' not found after attempting disk load.")
                    raise CacheMissError(k)

    def exists(self, k):
        """
        Thread-safe exists check.
        """
        with self._lock:
            return super().exists(k)

    def evict(self, n=1):
        """
        Thread-safe eviction.
        """
        with self._lock:
            super().evict(n)

    def load(self):
        """
        Loads the cache state (_cache dictionary and _key_list) from the
        specified filepath using pickle. Handles file not found and
        deserialization errors.
        Thread-safe operation.
        """
        with self._lock:
            if not os.path.exists(self.filepath) or os.path.getsize(self.filepath) == 0:
                logging.info(f"Cache file not found or empty, starting fresh: {self.filepath}")
                return  # Start with an empty cache

            try:
                with gzip.open(self.filepath, "rb") as f:  # Use binary mode 'rb' for pickle
                    loaded_data = pickle.load(f)

                if not isinstance(loaded_data, dict) or "_cache" not in loaded_data or "_key_list" not in loaded_data:
                    logging.warning(f"Invalid cache file format found: {self.filepath}. Starting fresh.")
                    self._cache = {}
                    self._key_list = []
                    return

                # Directly assign loaded data as pickle handles complex objects
                self._cache = loaded_data["_cache"]
                self._key_list = loaded_data["_key_list"]

                # Ensure consistency after loading (LRU eviction)
                if len(self._key_list) > self._max_keys:
                    self.evict(len(self._key_list) - self._max_keys)
                logging.info(f"Cache loaded successfully from {self.filepath} using pickle.")

            except (gzip.BadGzipFile, pickle.UnpicklingError, EOFError, TypeError, Exception) as e:  # Catch pickle errors
                logging.error(f"Error loading cache file {self.filepath} (pickle/Gzip): {e}. Starting fresh.")
                self._cache = {}
                self._key_list = []
            except OSError as e:
                logging.error(f"OS error loading cache file {self.filepath}: {e}. Starting fresh.")
                self._cache = {}
                self._key_list = []

    def save(self):
        """
        Saves the current cache state (_cache dictionary and _key_list) to the
        specified filepath using pickle. Creates parent directories if needed.
        Thread-safe operation.
        """
        with self._lock:
            try:
                # Ensure parent directory exists
                parent_dir = os.path.dirname(self.filepath)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)

                # Save cache and key list to gzipped pickle file
                data_to_save = {"_cache": self._cache, "_key_list": self._key_list}
                with gzip.open(self.filepath, "wb") as f:  # Use binary mode 'wb' for pickle
                    pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)  # Use highest protocol

                logging.info(f"Cache saved successfully to {self.filepath} using pickle.")

            except (OSError, pickle.PicklingError, Exception) as e:  # Catch pickle errors
                logging.error(f"Error saving cache file {self.filepath}: {e}")


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
