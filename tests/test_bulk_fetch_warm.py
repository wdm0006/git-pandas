"""
Tests for the bulk_fetch_and_warm functionality in ProjectDirectory class.
"""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from git import Repo

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache


class TestBulkFetchAndWarm(unittest.TestCase):
    """Test cases for ProjectDirectory.bulk_fetch_and_warm method."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache = EphemeralCache(max_keys=100)

        # Create mock repositories
        self.mock_repo1 = Mock(spec=Repository)
        self.mock_repo1.repo_name = "repo1"
        self.mock_repo1.safe_fetch_remote = Mock()
        self.mock_repo1.warm_cache = Mock()

        self.mock_repo2 = Mock(spec=Repository)
        self.mock_repo2.repo_name = "repo2"
        self.mock_repo2.safe_fetch_remote = Mock()
        self.mock_repo2.warm_cache = Mock()

        self.mock_repo3 = Mock(spec=Repository)
        self.mock_repo3.repo_name = "repo3"
        self.mock_repo3.safe_fetch_remote = Mock()
        self.mock_repo3.warm_cache = Mock()

        # Create ProjectDirectory with mock repositories
        self.project_dir = ProjectDirectory(working_dir=[], cache_backend=self.cache)
        self.project_dir.repos = [self.mock_repo1, self.mock_repo2, self.mock_repo3]

    def tearDown(self):
        """Clean up test fixtures."""
        try:
            import shutil

            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def test_bulk_fetch_and_warm_no_repos(self):
        """Test bulk operations with no repositories."""
        project_dir = ProjectDirectory(working_dir=[], cache_backend=self.cache)
        project_dir.repos = []

        result = project_dir.bulk_fetch_and_warm()

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 0)
        self.assertEqual(result["fetch_results"], {})
        self.assertEqual(result["cache_results"], {})
        self.assertGreaterEqual(result["execution_time"], 0)

    def test_bulk_fetch_and_warm_fetch_only(self):
        """Test bulk fetch without cache warming."""
        # Setup mock fetch results
        self.mock_repo1.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": True,
            "message": "Fetched successfully",
        }
        self.mock_repo2.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": False,
            "message": "Up to date",
        }
        self.mock_repo3.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": False,
            "changes_available": False,
            "message": "No remotes",
        }

        result = self.project_dir.bulk_fetch_and_warm(fetch_remote=True, warm_cache=False)

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 3)
        self.assertEqual(len(result["fetch_results"]), 3)
        self.assertEqual(len(result["cache_results"]), 0)
        self.assertEqual(result["summary"]["fetch_successful"], 3)
        self.assertEqual(result["summary"]["fetch_failed"], 0)
        self.assertEqual(result["summary"]["repositories_with_remotes"], 2)  # Only 2 have remotes

        # Verify fetch was called for all repos
        self.mock_repo1.safe_fetch_remote.assert_called_once()
        self.mock_repo2.safe_fetch_remote.assert_called_once()
        self.mock_repo3.safe_fetch_remote.assert_called_once()
        self.mock_repo1.warm_cache.assert_not_called()
        self.mock_repo2.warm_cache.assert_not_called()
        self.mock_repo3.warm_cache.assert_not_called()

    def test_bulk_fetch_and_warm_cache_only(self):
        """Test bulk cache warming without fetching."""
        # Setup mock cache results
        self.mock_repo1.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["commit_history", "branches"],
            "cache_entries_created": 5,
            "execution_time": 1.2,
        }
        self.mock_repo2.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["commit_history", "tags"],
            "cache_entries_created": 3,
            "execution_time": 0.8,
        }
        self.mock_repo3.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["blame"],
            "cache_entries_created": 2,
            "execution_time": 0.5,
        }

        result = self.project_dir.bulk_fetch_and_warm(fetch_remote=False, warm_cache=True)

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 3)
        self.assertEqual(len(result["fetch_results"]), 0)
        self.assertEqual(len(result["cache_results"]), 3)
        self.assertEqual(result["summary"]["cache_successful"], 3)
        self.assertEqual(result["summary"]["cache_failed"], 0)
        self.assertEqual(result["summary"]["total_cache_entries_created"], 10)  # 5+3+2

        # Verify cache warming was called for all repos
        self.mock_repo1.safe_fetch_remote.assert_not_called()
        self.mock_repo2.safe_fetch_remote.assert_not_called()
        self.mock_repo3.safe_fetch_remote.assert_not_called()
        self.mock_repo1.warm_cache.assert_called_once()
        self.mock_repo2.warm_cache.assert_called_once()
        self.mock_repo3.warm_cache.assert_called_once()

    def test_bulk_fetch_and_warm_both_operations(self):
        """Test bulk operations with both fetch and cache warming."""
        # Setup mock results
        self.mock_repo1.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": True,
        }
        self.mock_repo1.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["commit_history"],
            "cache_entries_created": 2,
            "execution_time": 0.5,
        }

        self.mock_repo2.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": False,
            "changes_available": False,
        }
        self.mock_repo2.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["branches"],
            "cache_entries_created": 1,
            "execution_time": 0.3,
        }

        self.mock_repo3.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": False,
        }
        self.mock_repo3.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["tags"],
            "cache_entries_created": 1,
            "execution_time": 0.4,
        }

        result = self.project_dir.bulk_fetch_and_warm(fetch_remote=True, warm_cache=True)

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 3)
        self.assertEqual(len(result["fetch_results"]), 3)
        self.assertEqual(len(result["cache_results"]), 3)

        # Verify both operations were called
        self.mock_repo1.safe_fetch_remote.assert_called_once()
        self.mock_repo1.warm_cache.assert_called_once()
        self.mock_repo2.safe_fetch_remote.assert_called_once()
        self.mock_repo2.warm_cache.assert_called_once()
        self.mock_repo3.safe_fetch_remote.assert_called_once()
        self.mock_repo3.warm_cache.assert_called_once()

    def test_bulk_fetch_and_warm_fetch_failure(self):
        """Test handling of fetch failures."""
        # Setup one successful and one failed fetch
        self.mock_repo1.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": False,
        }
        self.mock_repo2.safe_fetch_remote.return_value = {
            "success": False,
            "remote_exists": True,
            "error": "Network error",
        }
        self.mock_repo3.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": False,
            "changes_available": False,
        }

        result = self.project_dir.bulk_fetch_and_warm(fetch_remote=True)

        self.assertTrue(result["success"])  # Should still be successful overall
        self.assertEqual(result["summary"]["fetch_successful"], 2)  # repo1 and repo3 succeed
        self.assertEqual(result["summary"]["fetch_failed"], 1)  # repo2 fails

    def test_bulk_fetch_and_warm_cache_failure(self):
        """Test handling of cache warming failures."""
        # Setup one successful and one failed cache warming
        self.mock_repo1.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["branches"],
            "cache_entries_created": 1,
        }
        self.mock_repo2.warm_cache.return_value = {
            "success": False,
            "methods_executed": [],
            "errors": ["Method failed"],
        }
        self.mock_repo3.warm_cache.return_value = {
            "success": True,
            "methods_executed": ["tags"],
            "cache_entries_created": 2,
        }

        result = self.project_dir.bulk_fetch_and_warm(warm_cache=True)

        self.assertTrue(result["success"])  # Should still be successful overall
        self.assertEqual(result["summary"]["cache_successful"], 2)  # repo1 and repo3 succeed
        self.assertEqual(result["summary"]["cache_failed"], 1)  # repo2 fails

    def test_bulk_fetch_and_warm_custom_parameters(self):
        """Test bulk operations with custom parameters."""
        self.mock_repo1.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": False,
        }
        self.mock_repo1.warm_cache.return_value = {"success": True, "cache_entries_created": 1}
        self.mock_repo2.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": False,
            "changes_available": False,
        }
        self.mock_repo2.warm_cache.return_value = {"success": True, "cache_entries_created": 0}
        self.mock_repo3.safe_fetch_remote.return_value = {
            "success": True,
            "remote_exists": True,
            "changes_available": True,
        }
        self.mock_repo3.warm_cache.return_value = {"success": True, "cache_entries_created": 2}

        result = self.project_dir.bulk_fetch_and_warm(
            fetch_remote=True,
            warm_cache=True,
            remote_name="upstream",
            prune=True,
            dry_run=True,
            cache_methods=["branches", "tags"],
            limit=50,
        )

        self.assertTrue(result["success"])

        # Verify parameters were passed correctly
        self.mock_repo1.safe_fetch_remote.assert_called_once_with(remote_name="upstream", prune=True, dry_run=True)
        self.mock_repo1.warm_cache.assert_called_once_with(methods=["branches", "tags"], limit=50)

        # Also verify repo2 and repo3 got the same parameters
        self.mock_repo2.safe_fetch_remote.assert_called_once_with(remote_name="upstream", prune=True, dry_run=True)
        self.mock_repo3.safe_fetch_remote.assert_called_once_with(remote_name="upstream", prune=True, dry_run=True)

    def test_bulk_fetch_and_warm_no_operations(self):
        """Test bulk operations with no operations enabled."""
        result = self.project_dir.bulk_fetch_and_warm(fetch_remote=False, warm_cache=False)

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 3)
        self.assertEqual(len(result["fetch_results"]), 0)
        self.assertEqual(len(result["cache_results"]), 0)

        # Verify no methods were called
        self.mock_repo1.safe_fetch_remote.assert_not_called()
        self.mock_repo1.warm_cache.assert_not_called()

    def test_bulk_fetch_and_warm_parallel_execution(self):
        """Test parallel execution when joblib is available."""
        # Mock the parallel execution inside the method
        mock_parallel_class = Mock()
        mock_parallel_instance = Mock()
        mock_parallel_class.return_value = mock_parallel_instance
        mock_parallel_instance.return_value = [
            {"repo_name": "repo1", "success": True, "fetch_result": None, "cache_result": None, "error": None},
            {"repo_name": "repo2", "success": True, "fetch_result": None, "cache_result": None, "error": None},
            {"repo_name": "repo3", "success": True, "fetch_result": None, "cache_result": None, "error": None},
        ]

        with (
            patch("gitpandas.project._has_joblib", True),
            patch("joblib.Parallel", mock_parallel_class) as mock_parallel,
            patch("joblib.delayed", Mock()),
        ):
            result = self.project_dir.bulk_fetch_and_warm(parallel=True)

            self.assertTrue(result["success"])
            mock_parallel.assert_called_once_with(n_jobs=-1, backend="threading", verbose=0)

    def test_bulk_fetch_and_warm_sequential_execution(self):
        """Test sequential execution when joblib is not available."""
        with patch("gitpandas.project._has_joblib", False):
            result = self.project_dir.bulk_fetch_and_warm(parallel=True)  # Should fall back to sequential

            self.assertTrue(result["success"])
            self.assertEqual(result["repositories_processed"], 3)

    def test_bulk_fetch_and_warm_unexpected_error(self):
        """Test handling of unexpected errors during processing."""
        # Setup one repo to raise an exception
        self.mock_repo1.safe_fetch_remote.side_effect = Exception("Unexpected error")
        self.mock_repo2.safe_fetch_remote.return_value = {"success": True, "remote_exists": True}
        self.mock_repo3.safe_fetch_remote.return_value = {"success": True, "remote_exists": False}

        result = self.project_dir.bulk_fetch_and_warm(fetch_remote=True)

        self.assertTrue(result["success"])  # Should still succeed for repo2
        self.assertEqual(result["repositories_processed"], 3)


class TestBulkFetchAndWarmIntegration(unittest.TestCase):
    """Integration tests for bulk_fetch_and_warm using real git repositories."""

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

    def test_bulk_fetch_and_warm_real_repositories(self):
        """Test bulk operations on real git repositories."""
        # Create multiple test repositories
        repo_paths = []
        for i in range(2):
            repo_path = os.path.join(self.temp_dir, f"repo_{i}")
            os.makedirs(repo_path)

            git_repo = Repo.init(repo_path)

            # Create initial commit
            test_file = os.path.join(repo_path, "test.txt")
            with open(test_file, "w") as f:
                f.write(f"test content {i}")

            git_repo.index.add(["test.txt"])
            git_repo.index.commit(f"Initial commit {i}")

            repo_paths.append(repo_path)

        # Create ProjectDirectory with real repositories
        cache = EphemeralCache(max_keys=100)
        project_dir = ProjectDirectory(working_dir=repo_paths, cache_backend=cache)

        # Test bulk cache warming (no fetch since these are local repos without remotes)
        result = project_dir.bulk_fetch_and_warm(
            fetch_remote=True,  # Will fail gracefully - no remotes
            warm_cache=True,
            cache_methods=["branches", "list_files"],
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 2)
        self.assertEqual(result["summary"]["cache_successful"], 2)
        self.assertEqual(result["summary"]["fetch_failed"], 2)  # No remotes

        # Verify cache has entries
        self.assertGreater(len(cache._cache), 0)

    def test_bulk_fetch_and_warm_mixed_repository_types(self):
        """Test bulk operations with mixed repository types."""
        # Create one local repo without remotes
        repo_path = os.path.join(self.temp_dir, "local_repo")
        os.makedirs(repo_path)

        git_repo = Repo.init(repo_path)
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        git_repo.index.add(["test.txt"])
        git_repo.index.commit("Initial commit")

        # Create another repo with a fake remote
        repo_path2 = os.path.join(self.temp_dir, "remote_repo")
        os.makedirs(repo_path2)

        git_repo2 = Repo.init(repo_path2)
        test_file2 = os.path.join(repo_path2, "test2.txt")
        with open(test_file2, "w") as f:
            f.write("test content 2")
        git_repo2.index.add(["test2.txt"])
        git_repo2.index.commit("Initial commit 2")
        git_repo2.create_remote("origin", "https://github.com/test/repo.git")

        # Create ProjectDirectory
        cache = EphemeralCache(max_keys=100)
        project_dir = ProjectDirectory(working_dir=[repo_path, repo_path2], cache_backend=cache)

        # Test bulk operations
        with patch("git.Remote.fetch", return_value=[]):  # Mock successful fetch
            result = project_dir.bulk_fetch_and_warm(fetch_remote=True, warm_cache=True, cache_methods=["branches"])

        self.assertTrue(result["success"])
        self.assertEqual(result["repositories_processed"], 2)
        self.assertEqual(result["summary"]["cache_successful"], 2)
        self.assertEqual(result["summary"]["repositories_with_remotes"], 1)


if __name__ == "__main__":
    unittest.main()
