"""
Tests for the safe_fetch_remote functionality in Repository class.
"""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from git import GitCommandError, Repo

from gitpandas import Repository
from gitpandas.cache import EphemeralCache


class TestSafeFetchRemote(unittest.TestCase):
    """Test cases for Repository.safe_fetch_remote method."""

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

    def test_safe_fetch_remote_no_remotes(self):
        """Test fetch when repository has no remotes."""
        self.mock_repo.remotes = []

        result = self.repository.safe_fetch_remote()

        self.assertFalse(result["success"])
        self.assertFalse(result["remote_exists"])
        self.assertFalse(result["changes_available"])
        self.assertIn("No remotes configured", result["message"])
        self.assertIsNone(result["error"])

    def test_safe_fetch_remote_invalid_remote_name(self):
        """Test fetch with invalid remote name."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        self.mock_repo.remotes = [mock_origin]

        result = self.repository.safe_fetch_remote(remote_name="nonexistent")

        self.assertFalse(result["success"])
        self.assertFalse(result["remote_exists"])
        self.assertFalse(result["changes_available"])
        self.assertIn("Remote 'nonexistent' not found", result["message"])
        self.assertIn("origin", result["message"])

    def test_safe_fetch_remote_dry_run_success(self):
        """Test successful dry run fetch."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        mock_origin.url = "https://github.com/test/repo.git"
        mock_origin.refs = [Mock(), Mock(), Mock()]  # 3 refs

        self.mock_repo.remotes = [mock_origin]
        self.mock_repo.remote.return_value = mock_origin

        result = self.repository.safe_fetch_remote(dry_run=True)

        self.assertTrue(result["success"])
        self.assertTrue(result["remote_exists"])
        self.assertFalse(result["changes_available"])  # Dry run doesn't set this
        self.assertIn("Dry run", result["message"])
        self.assertIn("3 refs", result["message"])
        self.assertIsNone(result["error"])

    def test_safe_fetch_remote_dry_run_failure(self):
        """Test dry run fetch failure."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        mock_origin.refs = Mock(side_effect=Exception("Network error"))

        self.mock_repo.remotes = [mock_origin]
        self.mock_repo.remote.return_value = mock_origin

        result = self.repository.safe_fetch_remote(dry_run=True)

        self.assertFalse(result["success"])
        self.assertTrue(result["remote_exists"])
        self.assertFalse(result["changes_available"])
        self.assertIn("Dry run failed", result["error"])

    def test_safe_fetch_remote_success_with_changes(self):
        """Test successful fetch with changes available."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        mock_origin.url = "https://github.com/test/repo.git"

        # Mock fetch info objects
        mock_fetch_info_1 = Mock()
        mock_fetch_info_1.ref = Mock()
        mock_fetch_info_1.ref.name = "refs/remotes/origin/main"

        mock_fetch_info_2 = Mock()
        mock_fetch_info_2.ref = Mock()
        mock_fetch_info_2.ref.name = "refs/remotes/origin/dev"

        mock_origin.fetch.return_value = [mock_fetch_info_1, mock_fetch_info_2]

        self.mock_repo.remotes = [mock_origin]
        self.mock_repo.remote.return_value = mock_origin

        result = self.repository.safe_fetch_remote()

        self.assertTrue(result["success"])
        self.assertTrue(result["remote_exists"])
        self.assertTrue(result["changes_available"])
        self.assertIn("Successfully fetched 2 updates", result["message"])
        self.assertIsNone(result["error"])

    def test_safe_fetch_remote_success_no_changes(self):
        """Test successful fetch with no changes."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        mock_origin.fetch.return_value = []  # No changes

        self.mock_repo.remotes = [mock_origin]
        self.mock_repo.remote.return_value = mock_origin

        result = self.repository.safe_fetch_remote()

        self.assertTrue(result["success"])
        self.assertTrue(result["remote_exists"])
        self.assertFalse(result["changes_available"])
        self.assertIn("up to date", result["message"])
        self.assertIsNone(result["error"])

    def test_safe_fetch_remote_fetch_failure(self):
        """Test fetch failure."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        mock_origin.fetch.side_effect = GitCommandError("git fetch", "Network error")

        self.mock_repo.remotes = [mock_origin]
        self.mock_repo.remote.return_value = mock_origin

        result = self.repository.safe_fetch_remote()

        self.assertFalse(result["success"])
        self.assertTrue(result["remote_exists"])
        self.assertFalse(result["changes_available"])
        self.assertIn("Fetch failed", result["error"])

    def test_safe_fetch_remote_with_prune(self):
        """Test fetch with prune option."""
        mock_origin = Mock()
        mock_origin.name = "origin"
        mock_origin.fetch.return_value = []

        self.mock_repo.remotes = [mock_origin]
        self.mock_repo.remote.return_value = mock_origin

        result = self.repository.safe_fetch_remote(prune=True)

        self.assertTrue(result["success"])
        mock_origin.fetch.assert_called_once_with(prune=True)

    def test_safe_fetch_remote_custom_remote_name(self):
        """Test fetch with custom remote name."""
        mock_upstream = Mock()
        mock_upstream.name = "upstream"
        mock_upstream.fetch.return_value = []

        self.mock_repo.remotes = [mock_upstream]
        self.mock_repo.remote.return_value = mock_upstream

        result = self.repository.safe_fetch_remote(remote_name="upstream")

        self.assertTrue(result["success"])
        self.mock_repo.remote.assert_called_once_with("upstream")

    def test_safe_fetch_remote_unexpected_error(self):
        """Test handling of unexpected errors."""
        self.mock_repo.remotes = Mock(side_effect=Exception("Unexpected error"))

        result = self.repository.safe_fetch_remote()

        self.assertFalse(result["success"])
        self.assertIn("Unexpected error", result["error"])


class TestSafeFetchRemoteIntegration(unittest.TestCase):
    """Integration tests for safe_fetch_remote using real git repositories."""

    def setUp(self):
        """Set up test fixtures with real repositories."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache = EphemeralCache(max_keys=50)

    def tearDown(self):
        """Clean up test fixtures."""
        try:
            import shutil

            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass

    def test_safe_fetch_remote_local_repository(self):
        """Test fetch on a local repository without remotes."""
        # Create a local git repository
        repo_path = os.path.join(self.temp_dir, "local_repo")
        os.makedirs(repo_path)

        git_repo = Repo.init(repo_path)

        # Create initial commit
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        git_repo.index.add(["test.txt"])
        git_repo.index.commit("Initial commit")

        # Create Repository instance
        repository = Repository(working_dir=repo_path, cache_backend=self.cache)

        # Test fetch (should fail gracefully - no remotes)
        result = repository.safe_fetch_remote()

        self.assertFalse(result["success"])
        self.assertFalse(result["remote_exists"])
        self.assertIn("No remotes configured", result["message"])

    @patch("git.Remote.fetch")
    def test_safe_fetch_remote_with_mocked_network(self, mock_fetch):
        """Test fetch with mocked network operations."""
        # Create a local git repository
        repo_path = os.path.join(self.temp_dir, "remote_repo")
        os.makedirs(repo_path)

        git_repo = Repo.init(repo_path)

        # Create initial commit
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        git_repo.index.add(["test.txt"])
        git_repo.index.commit("Initial commit")

        # Add a fake remote
        git_repo.create_remote("origin", "https://github.com/test/repo.git")

        # Mock successful fetch
        mock_fetch.return_value = []

        # Create Repository instance
        repository = Repository(working_dir=repo_path, cache_backend=self.cache)

        # Test fetch
        result = repository.safe_fetch_remote()

        self.assertTrue(result["success"])
        self.assertTrue(result["remote_exists"])
        self.assertFalse(result["changes_available"])


if __name__ == "__main__":
    unittest.main()
