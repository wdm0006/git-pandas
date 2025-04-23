import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from git import Repo as GitRepo  # Use alias to avoid name clash
from gitpandas.repository import Repository
from gitpandas.cache import DiskCache, CacheMissError
from gitpandas.cache import EphemeralCache, multicache


class TestMulticache(unittest.TestCase):

    def setUp(self):
        self.cache = EphemeralCache()

    class TestClass:
        def __init__(self, cache):
            self.cache_backend = cache
            self.repo_name = "test_repo"
            self.call_count = 0

        @multicache(key_prefix="test_method_", key_list=["a", "b"])
        def cached_method(self, a, b, force_refresh=False): # Add force_refresh param
            self.call_count += 1
            return a + b * self.call_count # Return value depends on call count

    def test_multicache_force_refresh(self):
        """Test the force_refresh functionality of the multicache decorator."""
        # Use MagicMock to spy on cache methods
        mock_cache = MagicMock(spec=EphemeralCache)
        mock_cache._cache = {} # Simulate internal state if needed by methods
        mock_cache._key_list = []
        mock_cache._max_keys = 100

        # Real methods needed for the test
        mock_cache.set.side_effect = EphemeralCache.set.__get__(mock_cache, EphemeralCache)
        mock_cache.get.side_effect = EphemeralCache.get.__get__(mock_cache, EphemeralCache)
        mock_cache.exists.side_effect = EphemeralCache.exists.__get__(mock_cache, EphemeralCache)

        # Mock repo_name attribute expected by the decorator
        test_obj = self.TestClass(cache=mock_cache)
        test_obj.repo_name = "mock_repo" # Ensure repo_name exists

        # --- First Call --- (Cache Miss)
        result1 = test_obj.cached_method(a=1, b=2)
        self.assertEqual(result1, 1 + 2 * 1) # Calculation uses call_count = 1
        self.assertEqual(test_obj.call_count, 1)
        mock_cache.get.assert_called_once() # Attempted cache read
        mock_cache.set.assert_called_once() # Cache write after miss

        # Reset mocks for next call
        mock_cache.get.reset_mock()
        mock_cache.set.reset_mock()

        # --- Second Call --- (Cache Hit)
        result2 = test_obj.cached_method(a=1, b=2)
        self.assertEqual(result2, 1 + 2 * 1) # Should return cached value from call 1
        self.assertEqual(test_obj.call_count, 1) # Method not called again
        mock_cache.get.assert_called_once() # Successful cache read
        mock_cache.set.assert_not_called() # No cache write on hit

        # Reset mocks
        mock_cache.get.reset_mock()
        mock_cache.set.reset_mock()

        # --- Third Call with force_refresh=True --- (Cache Write)
        result3 = test_obj.cached_method(a=1, b=2, force_refresh=True)
        self.assertEqual(result3, 1 + 2 * 2) # Calculation uses call_count = 2
        self.assertEqual(test_obj.call_count, 2) # Method WAS called again
        mock_cache.get.assert_not_called() # Cache read was skipped
        mock_cache.set.assert_called_once() # Cache write happened

        # Reset mocks
        mock_cache.get.reset_mock()
        mock_cache.set.reset_mock()

        # --- Fourth Call --- (Cache Hit with updated value)
        result4 = test_obj.cached_method(a=1, b=2)
        self.assertEqual(result4, 1 + 2 * 2) # Should return the value cached in the previous step
        self.assertEqual(test_obj.call_count, 2) # Method not called again
        mock_cache.get.assert_called_once() # Successful cache read
        mock_cache.set.assert_not_called() # No cache write on hit


class TestDiskCachePersistence(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory and git repository for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.temp_dir, "test_repo")
        self.cache_file_path = os.path.join(self.temp_dir, "test_cache.json.gz")

        # Initialize a simple git repository
        self.git_repo = GitRepo.init(self.repo_path)
        with open(os.path.join(self.repo_path, "test_file.txt"), "w") as f:
            f.write("Initial content.\n")
        self.git_repo.index.add(["test_file.txt"])
        self.git_repo.index.commit("Initial commit")
        # Get a specific commit hash to use as rev, avoiding 'HEAD' which skips caching
        self.test_rev = self.git_repo.head.commit.hexsha

    def tearDown(self):
        """Remove the temporary directory after tests."""
        shutil.rmtree(self.temp_dir)

    def test_persistence_and_reuse(self):
        """
        Test caching an operation, saving to disk, loading into a new
        instance, and hitting the cache on the second call.
        """
        # --- Instance 1: Populate and Save Cache ---

        # Mock the underlying GitPython blame call within the Repository.blame method
        # The actual method called inside Repository.blame is repo.repo.blame
        mock_target = 'git.repo.base.Repo.blame'

        with patch(mock_target) as mock_git_blame:
            # Configure the mock to return some plausible blame data
            # Each item in the list is a tuple (commit, list_of_lines)
            mock_commit = MagicMock()
            mock_commit.committer.name = "Test Committer"
            mock_commit.author.name = "Test Author"
            mock_git_blame.return_value = [(mock_commit, ["line1", "line2"])]

            # Create Repository with DiskCache
            cache1 = DiskCache(filepath=self.cache_file_path)
            repo1 = Repository(self.repo_path, cache_backend=cache1)

            # Call the cached method (blame) - Use the specific rev
            # Use arguments that won't trigger the skip_if condition
            result1 = repo1.blame(rev=self.test_rev, committer=True, by="repository")

            # Assert the underlying git blame was called
            mock_git_blame.assert_called_once()
            self.assertIsInstance(result1, pd.DataFrame)
            self.assertFalse(result1.empty)
            # Check if 'Test Committer' is in the index (assuming by='repository', committer=True)
            self.assertIn("Test Committer", result1.index)

            # Explicitly save the cache
            repo1.cache_backend.save()
            self.assertTrue(os.path.exists(self.cache_file_path)) # Verify file exists


        # --- Instance 2: Load Cache and Test Cache Hit ---

        # Reset the mock for the second instance
        with patch(mock_target) as mock_git_blame:
             # Configure the mock return value again (though it shouldn't be called)
            mock_commit = MagicMock()
            mock_commit.committer.name = "Test Committer"
            mock_commit.author.name = "Test Author"
            mock_git_blame.return_value = [(mock_commit, ["line1", "line2"])]

            # Create a NEW Repository and NEW DiskCache instance for the SAME path
            cache2 = DiskCache(filepath=self.cache_file_path) # This should load from the file
            repo2 = Repository(self.repo_path, cache_backend=cache2)

            # Call the exact same cached method again
            result2 = repo2.blame(rev=self.test_rev, committer=True, by="repository")

            # Assert the underlying git blame was NOT called this time (cache hit)
            mock_git_blame.assert_not_called()

            # Assert the result from the cache is identical to the first result
            pd.testing.assert_frame_equal(result1, result2)

            # Optional: Verify internal cache state (e.g., key exists in cache2._cache)
            # Construct the expected key based on multicache logic
            expected_key_part = f"blame_{repo2.repo_name}_" + "_".join([
                str(self.test_rev), # rev
                str(True),         # committer
                "repository",      # by
                str(None),         # ignore_globs
                str(None)          # include_globs
            ])
            # Need to find the exact key format, might need adjustment
            # This is a bit fragile, relying on internal key generation logic
            found_key = False
            for k in cache2._cache.keys():
                 if expected_key_part in k: # Check if the core part matches
                     found_key = True
                     break
            self.assertTrue(found_key, "Expected key not found in cache2 internal state")

    def test_repository_force_refresh(self):
        """
        Test the force_refresh functionality using a real Repository object
        and the blame method.
        """
        mock_target = 'git.repo.base.Repo.blame'
        mock_calls = 0 # Manually track calls across patches

        # Configure a mock that returns slightly different data on subsequent calls
        # to ensure we can detect when the underlying function is actually run.
        mock_commit1 = MagicMock()
        mock_commit1.committer.name = "Test Committer 1"
        mock_commit1.author.name = "Test Author 1"
        mock_data1 = [(mock_commit1, ["line1"])]

        mock_commit2 = MagicMock()
        mock_commit2.committer.name = "Test Committer 2"
        mock_commit2.author.name = "Test Author 2"
        mock_data2 = [(mock_commit2, ["line1", "line2"])]

        # --- Instance 1 Setup ---
        cache1 = DiskCache(filepath=self.cache_file_path)
        repo1 = Repository(self.repo_path, cache_backend=cache1)

        # --- Call 1 (Cache Miss) ---
        with patch(mock_target, return_value=mock_data1) as mock_git_blame_1:
            print("Call 1: Normal")
            result1 = repo1.blame(rev=self.test_rev, committer=True, by="repository")
            mock_git_blame_1.assert_called_once()
            mock_calls += 1
            self.assertIn("Test Committer 1", result1.index)

        # --- Call 2 (Cache Hit) ---
        with patch(mock_target, return_value=mock_data1) as mock_git_blame_2:
            print("Call 2: Normal (expect cache hit)")
            result2 = repo1.blame(rev=self.test_rev, committer=True, by="repository")
            mock_git_blame_2.assert_not_called() # Should hit cache
            pd.testing.assert_frame_equal(result1, result2)
            self.assertIn("Test Committer 1", result2.index)

        # --- Call 3 (Force Refresh) ---
        with patch(mock_target, return_value=mock_data2) as mock_git_blame_3:
            print("Call 3: Force Refresh")
            result3 = repo1.blame(rev=self.test_rev, committer=True, by="repository", force_refresh=True)
            mock_git_blame_3.assert_called_once() # Should bypass cache read and call func
            mock_calls += 1
            self.assertIn("Test Committer 2", result3.index) # Check new data is present
            # Result 3 should be different from result 1 because the mock returned different data
            with self.assertRaises(AssertionError):
                 pd.testing.assert_frame_equal(result1, result3)

        # --- Call 4 (Cache Hit with refreshed data) ---
        with patch(mock_target, return_value=mock_data2) as mock_git_blame_4:
            print("Call 4: Normal (expect cache hit with refreshed data)")
            result4 = repo1.blame(rev=self.test_rev, committer=True, by="repository")
            mock_git_blame_4.assert_not_called() # Should hit cache again
            pd.testing.assert_frame_equal(result3, result4) # Should get data from call 3
            self.assertIn("Test Committer 2", result4.index)

        # Verify total mock calls
        self.assertEqual(mock_calls, 2, "Expected the underlying mock to be called exactly twice")


if __name__ == '__main__':
    unittest.main() 