import os
import tempfile
import pandas as pd
import pytest

from gitpandas.cache import DiskCache, multicache


class RepositoryMock:
    """Mock Repository class to test cache key generation consistency"""
    
    def __init__(self, working_dir="/mock/repo/path", cache_backend=None):
        self.working_dir = working_dir
        self.repo_name = working_dir  # Simulate how Repository sets repo_name
        self.cache_backend = cache_backend
        self.execution_count = 0
    
    @multicache(key_prefix="list_files", key_list=["directory", "filter_regex"])
    def list_files(self, directory=None, filter_regex=None, force_refresh=False):
        """Mock list_files method that demonstrates the caching behavior"""
        self.execution_count += 1
        return pd.DataFrame({
            "file": [f"file{i}.txt" for i in range(3)],
            "directory": [directory] * 3,
            "call_number": [self.execution_count] * 3
        })
    
    @multicache(key_prefix="complex_method", key_list=["param1", "param2", "param3"])
    def complex_method(self, param1=None, param2=None, param3=None, force_refresh=False):
        """Method with multiple parameters to test key generation with many parameters"""
        self.execution_count += 1
        return pd.DataFrame({
            "result": [f"result{i}" for i in range(2)],
            "param_values": [f"{param1}_{param2}_{param3}"] * 2,
            "call_number": [self.execution_count] * 2
        })


class TestCacheKeyConsistency:
    """Tests specifically focused on the cache key consistency issue"""
    
    @pytest.fixture
    def temp_cache_path(self):
        """Create temporary file path for cache"""
        fd, path = tempfile.mkstemp(suffix=".gz")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)
    
    def test_consistent_cache_keys(self, temp_cache_path):
        """Test that cache keys are consistent between method calls"""
        cache = DiskCache(filepath=temp_cache_path)
        repo = RepositoryMock(cache_backend=cache)
        
        # Capture cache keys generated
        original_set = cache.set
        captured_keys = []
        
        def mock_set(k, v):
            captured_keys.append(k)
            return original_set(k, v)
        
        cache.set = mock_set
        
        # First call
        repo.list_files(directory="src", filter_regex="*.py")
        first_key = captured_keys[0]
        
        # Clear captured keys
        captured_keys.clear()
        
        # Second call with identical parameters
        repo.list_files(directory="src", filter_regex="*.py")
        
        # No key should be captured on second call (cache hit)
        assert len(captured_keys) == 0
        
        # Force refresh should use the same key
        repo.list_files(directory="src", filter_regex="*.py", force_refresh=True)
        assert len(captured_keys) == 1
        assert captured_keys[0] == first_key
    
    def test_fix_resolves_reported_issue(self, temp_cache_path):
        """Test specifically addressing the reported issue"""
        cache = DiskCache(filepath=temp_cache_path)
        repo = RepositoryMock(working_dir="/absolute/path/to/repo", cache_backend=cache)
        
        # First call
        result1 = repo.list_files()
        assert repo.execution_count == 1
        
        # Second call to the same method (should use cache)
        result2 = repo.list_files()
        assert repo.execution_count == 1  # Should NOT increment
        
        # Results should match
        pd.testing.assert_frame_equal(result1, result2)
    
    def test_varied_path_formats(self, temp_cache_path):
        """Test with different path formats to ensure key consistency"""
        cache = DiskCache(filepath=temp_cache_path)
        
        # Different repo path formats
        repo1 = RepositoryMock(working_dir="/path/to/repo", cache_backend=cache)
        repo2 = RepositoryMock(working_dir="/path/to/repo/", cache_backend=cache)  # Extra slash
        
        # Capture all keys set in the cache
        original_set = cache.set
        captured_keys = []
        
        def mock_set(k, v):
            captured_keys.append(k)
            return original_set(k, v)
        
        cache.set = mock_set
        
        # Call method on first repo
        result1 = repo1.list_files(directory="src")
        assert repo1.execution_count == 1
        key1 = captured_keys[0]
        
        # Clear keys
        captured_keys.clear()
        
        # Call on second repo with same parameters - should generate a different key
        # due to different repo_name ("/path/to/repo" vs "/path/to/repo/")
        result2 = repo2.list_files(directory="src")
        assert repo2.execution_count == 1  # Should increment for repo2
        key2 = captured_keys[0]
        
        # Keys should be different because repo_name is different
        assert key1 != key2
        assert "/path/to/repo_" in key1
        assert "/path/to/repo/_" in key2
    
    def test_complex_key_generation(self, temp_cache_path):
        """Test key generation with complex parameters"""
        cache = DiskCache(filepath=temp_cache_path)
        repo = RepositoryMock(cache_backend=cache)
        
        # Capture keys
        original_set = cache.set
        captured_keys = []
        
        def mock_set(k, v):
            captured_keys.append(k)
            return original_set(k, v)
        
        cache.set = mock_set
        
        # Call with complex parameters
        repo.complex_method(param1="value1", param2="value2", param3="value3")
        
        # Check key format
        key = captured_keys[0]
        assert key.startswith("complex_method_")
        assert "_value1_" in key
        assert "_value2_" in key
        assert "_value3" in key
        
        # Call again with different order of parameters in the call
        # Python should normalize kwargs, so the key should be the same
        captured_keys.clear()
        repo.complex_method(param3="value3", param1="value1", param2="value2", force_refresh=True)
        
        # Key should be the same despite different parameter order
        assert captured_keys[0] == key 