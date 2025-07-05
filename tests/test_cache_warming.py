"""
Tests for the warm_cache functionality in Repository class.
"""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from git import Repo

from gitpandas import Repository
from gitpandas.cache import DiskCache, EphemeralCache


class TestWarmCache(unittest.TestCase):
    """Test cases for Repository.warm_cache method."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache = EphemeralCache(max_keys=50)

        # Create a mock repository
        self.mock_repo = Mock(spec=Repo)
        self.mock_repo.git_dir = self.temp_dir
        self.mock_repo.working_dir = self.temp_dir
        self.mock_repo.bare = False

        # Create Repository instance with mocked git repo
        self.repository = Repository(working_dir=None, cache_backend=self.cache, default_branch="main")
        self.repository.repo = self.mock_repo
        self.repository.git_dir = self.temp_dir

    def tearDown(self):
        """Clean up test fixtures."""
        try:
            import shutil

            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def test_warm_cache_no_cache_backend(self):
        """Test cache warming when no cache backend is configured."""
        repository = Repository(working_dir=None, cache_backend=None, default_branch="main")
        repository.repo = self.mock_repo
        repository.git_dir = self.temp_dir

        result = repository.warm_cache()

        self.assertTrue(result["success"])
        self.assertEqual(result["methods_executed"], [])
        self.assertEqual(result["methods_failed"], [])
        self.assertEqual(result["cache_entries_created"], 0)
        self.assertGreaterEqual(result["execution_time"], 0)
        self.assertEqual(result["errors"], [])

    def test_warm_cache_default_methods(self):
        """Test cache warming with default methods."""
        # Mock the methods to avoid actual git operations
        with (
            patch.object(self.repository, "commit_history", return_value=Mock()) as mock_commit_history,
            patch.object(self.repository, "branches", return_value=Mock()) as mock_branches,
            patch.object(self.repository, "tags", return_value=Mock()) as mock_tags,
            patch.object(self.repository, "blame", return_value=Mock()) as mock_blame,
            patch.object(self.repository, "file_detail", return_value=Mock()) as mock_file_detail,
            patch.object(self.repository, "list_files", return_value=Mock()) as mock_list_files,
        ):
            len(self.cache._cache)

            result = self.repository.warm_cache()

            self.assertTrue(result["success"])
            self.assertEqual(len(result["methods_executed"]), 6)
            self.assertIn("commit_history", result["methods_executed"])
            self.assertIn("branches", result["methods_executed"])
            self.assertIn("tags", result["methods_executed"])
            self.assertIn("blame", result["methods_executed"])
            self.assertIn("file_detail", result["methods_executed"])
            self.assertIn("list_files", result["methods_executed"])
            self.assertEqual(result["methods_failed"], [])
            self.assertGreaterEqual(result["execution_time"], 0)
            self.assertEqual(result["errors"], [])

            # Verify methods were called with appropriate arguments
            mock_commit_history.assert_called_once_with(limit=100)
            mock_branches.assert_called_once()
            mock_tags.assert_called_once()
            mock_blame.assert_called_once()
            mock_file_detail.assert_called_once()
            mock_list_files.assert_called_once()

    def test_warm_cache_custom_methods(self):
        """Test cache warming with custom method list."""
        with (
            patch.object(self.repository, "branches", return_value=Mock()) as mock_branches,
            patch.object(self.repository, "tags", return_value=Mock()) as mock_tags,
        ):
            result = self.repository.warm_cache(methods=["branches", "tags"])

            self.assertTrue(result["success"])
            self.assertEqual(result["methods_executed"], ["branches", "tags"])
            self.assertEqual(result["methods_failed"], [])
            self.assertEqual(result["errors"], [])

            mock_branches.assert_called_once()
            mock_tags.assert_called_once()

    def test_warm_cache_invalid_method(self):
        """Test cache warming with invalid method name."""
        result = self.repository.warm_cache(methods=["nonexistent_method"])

        self.assertFalse(result["success"])
        self.assertEqual(result["methods_executed"], [])
        self.assertEqual(result["methods_failed"], ["nonexistent_method"])
        self.assertIn("Method 'nonexistent_method' not found", result["errors"][0])

    def test_warm_cache_method_failure(self):
        """Test cache warming when a method fails."""
        with (
            patch.object(self.repository, "branches", side_effect=Exception("Git error")),
            patch.object(self.repository, "tags", return_value=Mock()),
        ):
            result = self.repository.warm_cache(methods=["branches", "tags"])

            self.assertTrue(result["success"])  # Still successful if at least one method works
            self.assertEqual(result["methods_executed"], ["tags"])
            self.assertEqual(result["methods_failed"], ["branches"])
            self.assertIn("Method 'branches' failed: Git error", result["errors"][0])

    def test_warm_cache_all_methods_fail(self):
        """Test cache warming when all methods fail."""
        with (
            patch.object(self.repository, "branches", side_effect=Exception("Error 1")),
            patch.object(self.repository, "tags", side_effect=Exception("Error 2")),
        ):
            result = self.repository.warm_cache(methods=["branches", "tags"])

            self.assertFalse(result["success"])
            self.assertEqual(result["methods_executed"], [])
            self.assertEqual(result["methods_failed"], ["branches", "tags"])
            self.assertEqual(len(result["errors"]), 2)

    def test_warm_cache_with_kwargs(self):
        """Test cache warming with additional keyword arguments."""
        with patch.object(self.repository, "commit_history", return_value=Mock()) as mock_commit_history:
            result = self.repository.warm_cache(
                methods=["commit_history"], limit=50, branch="main", ignore_globs=["*.log"]
            )

            self.assertTrue(result["success"])
            mock_commit_history.assert_called_once_with(limit=50, branch="main", ignore_globs=["*.log"])

    def test_warm_cache_special_method_defaults(self):
        """Test that special methods get appropriate default arguments."""
        with patch.object(self.repository, "file_change_rates", return_value=Mock()) as mock_file_change_rates:
            result = self.repository.warm_cache(methods=["file_change_rates"])

            self.assertTrue(result["success"])
            # Should get default limit of 100 for file_change_rates
            mock_file_change_rates.assert_called_once_with(limit=100)

    def test_warm_cache_cache_entries_tracking(self):
        """Test that cache entries created are properly tracked."""
        # Start with empty cache
        len(self.cache._cache)

        # Mock methods that will add entries to cache
        with patch.object(self.repository, "branches") as mock_branches:
            # Simulate adding cache entries
            def add_cache_entry(*args, **kwargs):
                self.cache.set("test_key_1", "test_value_1")
                return Mock()

            mock_branches.side_effect = add_cache_entry

            result = self.repository.warm_cache(methods=["branches"])

            self.assertTrue(result["success"])
            self.assertEqual(result["cache_entries_created"], 1)

    def test_warm_cache_execution_time_tracking(self):
        """Test that execution time is properly tracked."""
        with patch.object(self.repository, "branches", return_value=Mock()):
            result = self.repository.warm_cache(methods=["branches"])

            self.assertTrue(result["success"])
            self.assertGreaterEqual(result["execution_time"], 0)
            self.assertLess(result["execution_time"], 10)  # Should be quick for mocked methods


class TestWarmCacheIntegration(unittest.TestCase):
    """Integration tests for warm_cache using real git repositories."""

    def setUp(self):
        """Set up test fixtures with real repositories."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        try:
            import shutil

            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def test_warm_cache_real_repository(self):
        """Test cache warming on a real git repository."""
        # Create a local git repository
        repo_path = os.path.join(self.temp_dir, "test_repo")
        os.makedirs(repo_path)

        git_repo = Repo.init(repo_path)

        # Create some commits
        for i in range(3):
            test_file = os.path.join(repo_path, f"test_{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"test content {i}")

            git_repo.index.add([f"test_{i}.txt"])
            git_repo.index.commit(f"Commit {i}")

        # Create a cache and Repository instance
        cache = EphemeralCache(max_keys=50)
        repository = Repository(working_dir=repo_path, cache_backend=cache)

        # Test cache warming
        result = repository.warm_cache(methods=["commit_history", "branches", "list_files"], limit=10)

        self.assertTrue(result["success"])
        self.assertGreater(len(result["methods_executed"]), 0)
        self.assertEqual(result["methods_failed"], [])
        self.assertGreaterEqual(result["execution_time"], 0)

        # Verify cache has entries
        self.assertGreater(len(cache._cache), 0)

    def test_warm_cache_disk_cache(self):
        """Test cache warming with DiskCache."""
        # Create a local git repository
        repo_path = os.path.join(self.temp_dir, "test_repo")
        os.makedirs(repo_path)

        git_repo = Repo.init(repo_path)

        # Create initial commit
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        git_repo.index.add(["test.txt"])
        git_repo.index.commit("Initial commit")

        # Create a DiskCache and Repository instance
        cache_file = os.path.join(self.temp_dir, "test_cache.gz")
        cache = DiskCache(filepath=cache_file, max_keys=50)
        repository = Repository(working_dir=repo_path, cache_backend=cache)

        # Test cache warming
        result = repository.warm_cache(methods=["branches", "list_files"])

        self.assertTrue(result["success"])
        self.assertGreater(len(result["methods_executed"]), 0)

        # Verify cache file was created and has content
        self.assertTrue(os.path.exists(cache_file))
        self.assertGreater(os.path.getsize(cache_file), 0)

    def test_warm_cache_no_methods_specified(self):
        """Test cache warming with no methods specified (should use defaults)."""
        # Create a minimal git repository
        repo_path = os.path.join(self.temp_dir, "test_repo")
        os.makedirs(repo_path)

        git_repo = Repo.init(repo_path)

        # Create initial commit
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        git_repo.index.add(["test.txt"])
        git_repo.index.commit("Initial commit")

        # Create Repository instance
        cache = EphemeralCache(max_keys=50)
        repository = Repository(working_dir=repo_path, cache_backend=cache)

        # Test cache warming with defaults
        result = repository.warm_cache()

        self.assertTrue(result["success"])
        # Should execute several default methods
        self.assertGreater(len(result["methods_executed"]), 3)
        self.assertGreaterEqual(result["execution_time"], 0)


if __name__ == "__main__":
    unittest.main()
