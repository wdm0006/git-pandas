import pytest
import pandas as pd
import numpy as np
from gitpandas.cache import EphemeralCache, CacheMissException, multicache

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
        cache.set('key1', 'value1')
        assert cache.get('key1') == 'value1'
        
        # Test with pandas DataFrame
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        cache.set('key2', df)
        pd.testing.assert_frame_equal(cache.get('key2'), df)
        
        # Test with numpy array
        arr = np.array([1, 2, 3])
        cache.set('key3', arr)
        np.testing.assert_array_equal(cache.get('key3'), arr)
    
    def test_exists(self):
        """Test checking if a key exists in the cache."""
        cache = EphemeralCache()
        
        assert not cache.exists('key1')
        
        cache.set('key1', 'value1')
        assert cache.exists('key1')
        
        cache.get('key1')  # This should not remove the key
        assert cache.exists('key1')
    
    def test_cache_miss(self):
        """Test behavior when a key is not in the cache."""
        cache = EphemeralCache()
        
        with pytest.raises(CacheMissException):
            cache.get('nonexistent_key')
    
    def test_eviction(self):
        """Test that keys are evicted when the cache is full."""
        cache = EphemeralCache(max_keys=3)
        
        cache.set('key1', 'value1')
        cache.set('key2', 'value2')
        cache.set('key3', 'value3')
        
        # All keys should be in the cache
        assert cache.exists('key1')
        assert cache.exists('key2')
        assert cache.exists('key3')
        
        # Adding a new key should evict the oldest key (key1)
        cache.set('key4', 'value4')
        
        assert not cache.exists('key1')
        assert cache.exists('key2')
        assert cache.exists('key3')
        assert cache.exists('key4')
        
        # Accessing key2 should move it to the end of the list
        cache.get('key2')
        
        # Adding a new key should now evict key3 (not key2, since it was just accessed)
        cache.set('key5', 'value5')
        
        assert not cache.exists('key1')
        assert cache.exists('key2')
        assert not cache.exists('key3')
        assert cache.exists('key4')
        assert cache.exists('key5')
    
    def test_update_existing_key(self):
        """Test updating an existing key in the cache."""
        cache = EphemeralCache()
        
        cache.set('key1', 'value1')
        assert cache.get('key1') == 'value1'
        
        cache.set('key1', 'new_value')
        assert cache.get('key1') == 'new_value'


class TestMulticache:
    def test_multicache_decorator(self):
        """Test the multicache decorator with EphemeralCache."""
        cache = EphemeralCache()
        
        # Define a class with a cached method
        class TestClass:
            def __init__(self):
                self.cache_backend = cache
                self.repo_name = 'test_repo'
                self.call_count = 0
            
            @multicache(key_prefix='test_', key_list=['a', 'b'])
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
                self.repo_name = 'test_repo'
                self.call_count = 0
            
            @multicache(
                key_prefix='test_', 
                key_list=['a', 'b'],
                skip_if=lambda kwargs: kwargs.get('skip_cache', False)
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
                self.repo_name = 'test_repo'
                self.call_count = 0
            
            @multicache(key_prefix='test_', key_list=['a', 'b'])
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