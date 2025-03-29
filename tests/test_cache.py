from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from gitpandas.cache import CacheMissError, EphemeralCache, RedisDFCache, multicache


class TestEphemeralCache:
    def test_init(self):
        """Test initialization of EphemeralCache."""
        cache = EphemeralCache()
        assert cache._max_keys == 1000
        assert cache._cache == {}
        assert cache._key_list == []

        cache = EphemeralCache(max_keys=500)
        assert cache._max_keys == 500

    def test_set_get(self):
        """Test setting and getting values from the cache."""
        cache = EphemeralCache()

        # Test with simple values
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        # Test with pandas DataFrame
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        cache.set("key2", df)
        pd.testing.assert_frame_equal(cache.get("key2"), df)

        # Test with numpy array
        arr = np.array([1, 2, 3])
        cache.set("key3", arr)
        np.testing.assert_array_equal(cache.get("key3"), arr)

    def test_exists(self):
        """Test checking if a key exists in the cache."""
        cache = EphemeralCache()

        assert not cache.exists("key1")

        cache.set("key1", "value1")
        assert cache.exists("key1")

        cache.get("key1")  # This should not remove the key
        assert cache.exists("key1")

    def test_cache_miss(self):
        """Test behavior when a key is not in the cache."""
        cache = EphemeralCache()

        with pytest.raises(CacheMissError):
            cache.get("nonexistent_key")

    def test_eviction(self):
        """Test that keys are evicted when the cache is full."""
        cache = EphemeralCache(max_keys=3)

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # All keys should be in the cache
        assert cache.exists("key1")
        assert cache.exists("key2")
        assert cache.exists("key3")

        # Adding a new key should evict the oldest key (key1)
        cache.set("key4", "value4")

        assert not cache.exists("key1")
        assert cache.exists("key2")
        assert cache.exists("key3")
        assert cache.exists("key4")

        # Accessing key2 should move it to the end of the list
        cache.get("key2")

        # Adding a new key should now evict key3 (not key2, since it was just accessed)
        cache.set("key5", "value5")

        assert not cache.exists("key1")
        assert cache.exists("key2")
        assert not cache.exists("key3")
        assert cache.exists("key4")
        assert cache.exists("key5")

    def test_update_existing_key(self):
        """Test updating an existing key in the cache."""
        cache = EphemeralCache()

        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        cache.set("key1", "new_value")
        assert cache.get("key1") == "new_value"


class TestMulticache:
    def test_multicache_decorator(self):
        """Test the multicache decorator with EphemeralCache."""
        cache = EphemeralCache()

        # Define a class with a cached method
        class TestClass:
            def __init__(self):
                self.cache_backend = cache
                self.repo_name = "test_repo"
                self.call_count = 0

            @multicache(key_prefix="test_", key_list=["a", "b"])
            def cached_method(self, a, b):
                self.call_count += 1
                return a + b

        test_obj = TestClass()

        # First call should execute the method
        result1 = test_obj.cached_method(a=1, b=2)
        assert result1 == 3
        assert test_obj.call_count == 1

        # Second call with same args should use the cache
        result2 = test_obj.cached_method(a=1, b=2)
        assert result2 == 3
        assert test_obj.call_count == 1  # Call count should not increase

        # Call with different args should execute the method again
        result3 = test_obj.cached_method(a=2, b=3)
        assert result3 == 5
        assert test_obj.call_count == 2

    def test_multicache_skip_if(self):
        """Test the skip_if parameter of the multicache decorator."""
        cache = EphemeralCache()

        # Define a class with a cached method that skips caching in some cases
        class TestClass:
            def __init__(self):
                self.cache_backend = cache
                self.repo_name = "test_repo"
                self.call_count = 0

            @multicache(
                key_prefix="test_",
                key_list=["a", "b"],
                skip_if=lambda kwargs: kwargs.get("skip_cache", False),
            )
            def cached_method(self, a, b, skip_cache=False):
                self.call_count += 1
                return a + b

        test_obj = TestClass()

        # First call should execute the method and cache the result
        result1 = test_obj.cached_method(a=1, b=2)
        assert result1 == 3
        assert test_obj.call_count == 1

        # Second call with same args should use the cache
        result2 = test_obj.cached_method(a=1, b=2)
        assert result2 == 3
        assert test_obj.call_count == 1  # Call count should not increase

        # Call with skip_cache=True should skip the cache
        result3 = test_obj.cached_method(a=1, b=2, skip_cache=True)
        assert result3 == 3
        assert test_obj.call_count == 2  # Call count should increase

    def test_multicache_no_cache_backend(self):
        """Test the multicache decorator when no cache backend is provided."""

        # Define a class with a cached method but no cache backend
        class TestClass:
            def __init__(self):
                self.cache_backend = None
                self.repo_name = "test_repo"
                self.call_count = 0

            @multicache(key_prefix="test_", key_list=["a", "b"])
            def cached_method(self, a, b):
                self.call_count += 1
                return a + b

        test_obj = TestClass()

        # First call should execute the method
        result1 = test_obj.cached_method(a=1, b=2)
        assert result1 == 3
        assert test_obj.call_count == 1

        # Second call should also execute the method (no caching)
        result2 = test_obj.cached_method(a=1, b=2)
        assert result2 == 3
        assert test_obj.call_count == 2  # Call count should increase


@pytest.mark.redis
class TestRedisDFCache:
    """Tests for the RedisDFCache class.

    These tests use mocks to avoid requiring a Redis server.
    """

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_init(self, mock_redis):
        """Test initialization of RedisDFCache."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = []

        # Test with default parameters
        cache = RedisDFCache()
        assert cache._max_keys == 1000
        assert cache.ttl is None
        assert cache.prefix == "gitpandas_"

        # Test with custom parameters
        cache = RedisDFCache(host="redis.example.com", port=6380, db=5, max_keys=500, ttl=3600)
        assert cache._max_keys == 500
        assert cache.ttl == 3600

        # Verify Redis was initialized with correct parameters
        mock_redis.assert_called_with(host="redis.example.com", port=6380, db=5)

    @patch("gitpandas.cache._HAS_REDIS", False)
    def test_init_no_redis(self):
        """Test that ImportError is raised when redis is not installed."""
        with pytest.raises(ImportError):
            RedisDFCache()

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_set_get(self, mock_redis):
        """Test setting and getting values from the Redis cache."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = []
        mock_redis_instance.exists.return_value = True

        # Create a mock pickle.dumps return value
        mock_pickle_data = b"mock_pickled_data"

        # Create test data
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        # Mock pickle.dumps and loads
        with (
            patch("pickle.dumps", return_value=mock_pickle_data) as mock_dumps,
            patch("pickle.loads", return_value=df) as mock_loads,
        ):
            cache = RedisDFCache()

            # Test set
            cache.set("key1", df)
            mock_dumps.assert_called_once_with(df)
            mock_redis_instance.set.assert_called_once_with("gitpandas_key1", mock_pickle_data, ex=None)

            # Test get
            mock_redis_instance.get.return_value = mock_pickle_data
            result = cache.get("key1")
            mock_loads.assert_called_once_with(mock_pickle_data)
            pd.testing.assert_frame_equal(result, df)

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_exists(self, mock_redis):
        """Test checking if a key exists in the Redis cache."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = []

        cache = RedisDFCache()

        # Test key exists
        mock_redis_instance.exists.return_value = True
        assert cache.exists("key1")
        mock_redis_instance.exists.assert_called_with("gitpandas_key1")

        # Test key doesn't exist
        mock_redis_instance.exists.return_value = False
        assert not cache.exists("key2")

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_cache_miss(self, mock_redis):
        """Test behavior when a key is not in the Redis cache."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = []
        mock_redis_instance.exists.return_value = False

        cache = RedisDFCache()

        with pytest.raises(CacheMissError):
            cache.get("nonexistent_key")

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_eviction(self, mock_redis):
        """Test that keys are evicted when the Redis cache is full."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = []

        # Initialize cache with small max_keys
        cache = RedisDFCache(max_keys=2)

        # Add keys to the _key_list to simulate existing keys
        cache._key_list = ["gitpandas_key1", "gitpandas_key2"]

        # Adding a new key should trigger eviction of the oldest key
        cache.set("key3", "value3")

        # Check that key1 was evicted
        mock_redis_instance.delete.assert_called_once_with("gitpandas_key1")
        assert "gitpandas_key1" not in cache._key_list
        assert "gitpandas_key2" in cache._key_list
        assert "gitpandas_key3" in cache._key_list

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_ttl(self, mock_redis):
        """Test that TTL is correctly passed to Redis."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = []

        # Initialize cache with TTL
        cache = RedisDFCache(ttl=60)

        # Set a value
        cache.set("key1", "value1")

        # Verify that TTL was passed to Redis set command
        mock_redis_instance.set.assert_called_once_with(
            "gitpandas_key1",
            mock_redis_instance.set.call_args[0][1],  # Not checking actual serialized value
            ex=60,
        )

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_sync(self, mock_redis):
        """Test syncing with existing Redis keys."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        # Mock Redis keys response
        mock_redis_instance.keys.return_value = [b"gitpandas_key1", b"gitpandas_key2"]

        cache = RedisDFCache()

        # Sync should have been called during initialization
        assert cache._key_list == ["gitpandas_key1", "gitpandas_key2"]

        # Test explicit sync call with new keys
        mock_redis_instance.keys.return_value = [b"gitpandas_key1", b"gitpandas_key2", b"gitpandas_key3"]

        cache.sync()
        assert cache._key_list == ["gitpandas_key1", "gitpandas_key2", "gitpandas_key3"]

    @patch("gitpandas.cache._HAS_REDIS", True)
    @patch("gitpandas.cache.redis.StrictRedis")
    def test_purge(self, mock_redis):
        """Test purging all keys with the cache prefix."""
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        # Mock scan_iter to return some keys
        mock_redis_instance.scan_iter.return_value = [b"gitpandas_key1", b"gitpandas_key2"]

        cache = RedisDFCache()
        cache.purge()

        # Verify scan_iter was called with correct pattern
        mock_redis_instance.scan_iter.assert_called_once_with("gitpandas_*")

        # Verify each key was deleted
        assert mock_redis_instance.delete.call_count == 2
        mock_redis_instance.delete.assert_any_call(b"gitpandas_key1")
        mock_redis_instance.delete.assert_any_call(b"gitpandas_key2")
