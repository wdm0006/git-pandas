import pickle
import os
import logging
import json
import gzip
import pandas as pd

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

            # Check for force_refresh argument to bypass cache read but still write
            force_refresh = kwargs.get('force_refresh', False)

            # Generate the cache key (ensure force_refresh itself is not part of the key)
            # Assumes 'force_refresh' is not included in key_list by the caller
            key_parts = [str(kwargs.get(k)) for k in key_list]
            key = key_prefix + self.repo_name + "_".join(key_parts)
            logging.debug(f"Cache key generated for {key_prefix}: {key}")

            cache_hit = False
            # Try retrieving from cache if not forcing refresh
            if not force_refresh:
                try:
                    # Include DiskCache in the type check
                    if isinstance(self.cache_backend, (EphemeralCache, RedisDFCache, DiskCache)):
                        ret = self.cache_backend.get(key)
                        logging.debug(f"Cache hit for key: {key}")
                        cache_hit = True
                        return ret
                    else:
                        logging.warning(f"Cannot get from cache; unknown backend type: {type(self.cache_backend)}")
                        # Fall through to execute function if backend is unknown

                except CacheMissError:
                    logging.debug(f"Cache miss for key: {key}")
                    pass # Proceed to execute the function
                except Exception as e:
                    logging.error(f"Error getting cache for key {key}: {e}", exc_info=True)
                    # Proceed to execute function on cache read error

            # Execute the function if cache missed, force_refresh is True, or cache read failed
            if not cache_hit:
                logging.debug(f"Executing function for key: {key} (Force Refresh: {force_refresh})")
                ret = func(self, *args, **kwargs)

                # Store the result in the cache (always happens after function execution)
                try:
                    if isinstance(self.cache_backend, (EphemeralCache, RedisDFCache, DiskCache)):
                        self.cache_backend.set(key, ret)
                        logging.debug(f"Cache set for key: {key}")
                    else:
                        logging.warning(f"Cannot set cache; unknown backend type: {type(self.cache_backend)}")
                except Exception as e:
                    logging.error(f"Failed to set cache for key {key}: {e}", exc_info=True)
                    # Return the computed result even if caching fails

                return ret
            # This part should not be reachable if cache_hit was True earlier
            # If cache_hit was True, the function returned inside the 'if not force_refresh' block
            # If cache_hit was False, the function returned after the 'set' operation above.

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


class DiskCache(EphemeralCache):
    """
    An in-memory cache that can be persisted to disk using pickle.

    Inherits LRU eviction logic from EphemeralCache.
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
        self.load() # Attempt to load existing cache on initialization

    def load(self):
        """
        Loads the cache state (_cache dictionary and _key_list) from the
        specified filepath using pickle. Handles file not found and
        deserialization errors.
        """
        if not os.path.exists(self.filepath) or os.path.getsize(self.filepath) == 0:
            logging.info(f"Cache file not found or empty, starting fresh: {self.filepath}")
            return # Start with an empty cache

        try:
            with gzip.open(self.filepath, 'rt', encoding='utf-8') as f:
                loaded_data = json.load(f)

            if not isinstance(loaded_data, dict) or '_cache' not in loaded_data or '_key_list' not in loaded_data:
                logging.warning(f"Invalid cache file format found: {self.filepath}. Starting fresh.")
                self._cache = {}
                self._key_list = []
                return

            self._key_list = loaded_data['_key_list']
            raw_cache = loaded_data['_cache']
            self._cache = {}
            for key, value in raw_cache.items():
                try:
                    if isinstance(value, dict) and '__dataframe__' in value:
                        # Attempt to decode DataFrame from JSON string
                        self._cache[key] = pd.read_json(value['__dataframe__'], orient='split')
                    else:
                        # Store other JSON-native types directly
                        self._cache[key] = value
                except (ValueError, TypeError) as df_err:
                    logging.warning(f"Could not decode DataFrame for key '{key}' from cache file {self.filepath}: {df_err}. Skipping entry.")
                    # Remove corresponding key from key list if it exists
                    if key in self._key_list:
                         self._key_list.remove(key)
                    continue # Skip this problematic entry
            
            # Ensure consistency after loading (LRU eviction)
            if len(self._key_list) > self._max_keys:
                 self.evict(len(self._key_list) - self._max_keys)
            logging.info(f"Cache loaded successfully from {self.filepath} using JSON.")

        except (gzip.BadGzipFile, json.JSONDecodeError, EOFError, TypeError, Exception) as e:
            logging.error(f"Error loading cache file {self.filepath} (JSON/Gzip): {e}. Starting fresh.")
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
        """
        try:
            # Prepare cache for JSON serialization
            processed_cache = {}
            for key, value in self._cache.items():
                try:
                    if isinstance(value, pd.DataFrame):
                        # Special handling for DataFrames
                        processed_cache[key] = {"__dataframe__": value.to_json(orient='split', date_format='iso')}
                    else:
                        # Attempt to store other types; will fail if not JSON serializable
                        json.dumps(value) # Test serializability
                        processed_cache[key] = value
                except TypeError as e:
                    logging.warning(f"Could not serialize value for key '{key}' to JSON: {e}. Skipping entry.")
                    # Also remove from key list if it's there (prevent loading errors)
                    if key in self._key_list:
                        self._key_list.remove(key)
                    continue # Skip this unserializable entry

            # Ensure parent directory exists
            parent_dir = os.path.dirname(self.filepath)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            # Save processed cache and key list to gzipped JSON
            data_to_save = {
                '_cache': processed_cache,
                '_key_list': self._key_list
            }
            with gzip.open(self.filepath, 'wt', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=2) # Use indent for readability if needed

            logging.info(f"Cache saved successfully to {self.filepath} using JSON.")

        except (OSError, TypeError, Exception) as e:
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
