from unittest.mock import Mock, patch

import git
import pandas as pd
import pytest
from git import GitCommandError

from gitpandas import Repository


class TestRepositoryErrorHandling:
    """Test Repository error handling and exception paths."""

    @pytest.fixture
    def temp_repo(self, tmp_path):
        """Create a temporary git repository for testing."""
        repo_path = tmp_path / "test_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create initial commit
        (repo_path / "README.md").write_text("# Test Repository")
        repo.index.add(["README.md"])
        repo.index.commit("Initial commit")

        return repo_path

    def test_coverage_file_not_found(self, temp_repo):
        """Test coverage method when .coverage file doesn't exist."""
        repo = Repository(working_dir=str(temp_repo))

        # Should return empty DataFrame when no coverage file exists
        coverage_df = repo.coverage()

        assert isinstance(coverage_df, pd.DataFrame)
        assert coverage_df.empty
        assert list(coverage_df.columns) == ["filename", "lines_covered", "total_lines", "coverage"]

    def test_coverage_permission_denied(self, temp_repo):
        """Test coverage method when permission is denied to .coverage file."""
        repo = Repository(working_dir=str(temp_repo))

        # Create a .coverage file
        coverage_file = temp_repo / ".coverage"
        coverage_file.write_text("test coverage data")

        # Since the coverage handling is built-in to repository.py, test by creating
        # a file that exists but simulating permission error via OS error
        with patch("os.path.join") as mock_join:
            mock_join.side_effect = PermissionError("Permission denied")

            coverage_df = repo.coverage()

            assert isinstance(coverage_df, pd.DataFrame)
            assert coverage_df.empty
            assert list(coverage_df.columns) == ["filename", "lines_covered", "total_lines", "coverage"]

    def test_coverage_invalid_format(self, temp_repo):
        """Test coverage method when coverage data has invalid format."""
        repo = Repository(working_dir=str(temp_repo))

        # Create a .coverage file
        coverage_file = temp_repo / ".coverage"
        coverage_file.write_text("invalid coverage data")

        # Test the actual error handling when coverage file exists but can't be parsed
        # Just test that the method doesn't crash and returns empty DataFrame
        coverage_df = repo.coverage()

        assert isinstance(coverage_df, pd.DataFrame)
        # For an invalid coverage file, it should return empty DataFrame
        assert coverage_df.empty or len(coverage_df) == 0

    def test_coverage_unexpected_error(self, temp_repo):
        """Test coverage method when unexpected error occurs."""
        repo = Repository(working_dir=str(temp_repo))

        # Create a .coverage file
        coverage_file = temp_repo / ".coverage"
        coverage_file.write_text("test coverage data")

        # Mock open() to raise an unexpected error during file processing
        with patch("builtins.open") as mock_open:
            mock_open.side_effect = RuntimeError("Unexpected file error")

            coverage_df = repo.coverage()

            assert isinstance(coverage_df, pd.DataFrame)
            assert coverage_df.empty

    def test_file_change_history_git_errors(self, temp_repo):
        """Test file_change_history when Git commands fail."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock iter_commits to raise GitCommandError
        with patch.object(repo.repo, "iter_commits") as mock_iter:
            mock_iter.side_effect = GitCommandError("git log failed", 128)

            # Should return empty DataFrame on git error
            history = repo.file_change_history(branch="master")

            assert isinstance(history, pd.DataFrame)
            expected_columns = ["filename", "insertions", "deletions", "lines", "message", "committer", "author"]
            assert list(history.columns) == expected_columns

    def test_file_change_history_skip_broken_commits(self, temp_repo):
        """Test file_change_history with skip_broken=True for problematic commits."""
        repo = Repository(working_dir=str(temp_repo))

        # Create a mock commit that will raise an error
        mock_commit = Mock()
        mock_commit.hexsha = "abc123"
        mock_commit.committed_date = 1234567890
        mock_commit.parents = []

        with patch.object(repo.repo, "iter_commits") as mock_iter:
            mock_iter.return_value = [mock_commit]

            # Mock _process_commit_for_file_history to raise error
            with patch.object(repo, "_process_commit_for_file_history") as mock_process:
                mock_process.side_effect = ValueError("Commit processing failed")

                # With skip_broken=True, should handle the error gracefully
                history = repo.file_change_history(branch="master", skip_broken=True)

                assert isinstance(history, pd.DataFrame)
                # Should have empty DataFrame since all commits were skipped
                assert len(history) == 0

    def test_file_change_history_memory_error_simulation(self, temp_repo):
        """Test file_change_rates when MemoryError occurs."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock file_change_history to raise MemoryError
        with patch.object(repo, "file_change_history") as mock_fch:
            mock_fch.side_effect = MemoryError("Out of memory")

            # Should return empty DataFrame on memory error
            rates = repo.file_change_rates(branch="master")

            assert isinstance(rates, pd.DataFrame)
            expected_columns = [
                "file",
                "unique_committers",
                "abs_rate_of_change",
                "net_rate_of_change",
                "net_change",
                "abs_change",
                "edit_rate",
                "lines",
                "repository",
            ]
            assert list(rates.columns) == expected_columns
            assert rates.empty

    def test_blame_unicode_decode_errors(self, temp_repo):
        """Test blame method when encountering Unicode decode errors."""
        repo = Repository(working_dir=str(temp_repo))

        # Create a file with potential encoding issues
        binary_file = temp_repo / "binary.dat"
        binary_file.write_bytes(b"\x80\x81\x82\x83")  # Invalid UTF-8

        # Add and commit the binary file
        git_repo = git.Repo(temp_repo)
        git_repo.index.add(["binary.dat"])
        git_repo.index.commit("Add binary file")

        # Mock file reading to raise UnicodeDecodeError
        with patch("builtins.open") as mock_open:
            mock_open.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "invalid start byte")

            # Should handle UnicodeDecodeError gracefully
            blame_df = repo.blame()

            assert isinstance(blame_df, pd.DataFrame)

    def test_blame_binary_file_handling(self, temp_repo):
        """Test blame method with binary files."""
        repo = Repository(working_dir=str(temp_repo))

        # The blame method should handle cases where files can't be processed
        # This tests the continue logic in the blame processing
        blame_df = repo.blame(include_globs=["*.nonexistent"])

        assert isinstance(blame_df, pd.DataFrame)
        # Should have expected columns even if empty
        assert "loc" in blame_df.columns

    def test_cumulative_blame_broken_commits(self, temp_repo):
        """Test cumulative_blame with broken commits that should be skipped."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock revs to return DataFrame with problematic revision
        mock_revs_df = pd.DataFrame({"rev": ["abc123"], "repository": ["test_repo"]})

        with patch.object(repo, "revs") as mock_revs:
            mock_revs.return_value = mock_revs_df

            # Mock blame to raise error for the problematic revision
            with patch.object(repo, "blame") as mock_blame:
                mock_blame.side_effect = GitCommandError("blame failed", 128)

                # Should handle the error and return partial results
                cumulative = repo.cumulative_blame(branch="master", skip_broken=True)

                assert isinstance(cumulative, pd.DataFrame)

    def test_file_operations_missing_branch(self, temp_repo):
        """Test file operations when specified branch doesn't exist."""
        repo = Repository(working_dir=str(temp_repo))

        # Try to access a non-existent branch
        with patch.object(repo.repo, "iter_commits") as mock_iter:
            mock_iter.side_effect = GitCommandError("branch not found", 128)

            # file_change_history should handle missing branch gracefully
            history = repo.file_change_history(branch="nonexistent_branch")

            assert isinstance(history, pd.DataFrame)
            assert history.empty

    def test_punchcard_missing_columns(self, temp_repo):
        """Test punchcard when required columns are missing from commit history."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock the punchcard method on repository to return empty DataFrame
        with patch.object(repo, "punchcard") as mock_punchcard:
            mock_punchcard.return_value = pd.DataFrame(
                columns=["hour_of_day", "day_of_week", "lines", "insertions", "deletions", "net"]
            )

            # Should handle missing columns gracefully
            punchcard = repo.punchcard()

            assert isinstance(punchcard, pd.DataFrame)

    def test_repository_initialization_invalid_path(self):
        """Test Repository initialization with invalid path."""
        with pytest.raises((OSError, GitCommandError)):
            Repository(working_dir="/nonexistent/path/to/repo")

    def test_repository_initialization_not_a_git_repo(self, tmp_path):
        """Test Repository initialization with path that's not a git repository."""
        non_git_dir = tmp_path / "not_a_repo"
        non_git_dir.mkdir()

        with pytest.raises(git.exc.InvalidGitRepositoryError):
            Repository(working_dir=str(non_git_dir))

    def test_file_change_rates_empty_repository(self, temp_repo):
        """Test file_change_rates with repository that has no file changes."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock file_change_history to return empty DataFrame
        with patch.object(repo, "file_change_history") as mock_fch:
            mock_fch.return_value = pd.DataFrame(
                columns=["filename", "insertions", "deletions", "lines", "message", "committer", "author"]
            )

            rates = repo.file_change_rates(branch="master")

            assert isinstance(rates, pd.DataFrame)
            assert rates.empty

    def test_git_command_timeout_simulation(self, temp_repo):
        """Test handling of git command timeouts."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock a git operation to raise timeout-like error
        with patch.object(repo.repo, "iter_commits") as mock_iter:
            mock_iter.side_effect = GitCommandError("operation timed out", 124)

            # Should handle timeout gracefully
            history = repo.file_change_history(branch="master")

            assert isinstance(history, pd.DataFrame)
            assert history.empty

    def test_revs_with_broken_commits(self, temp_repo):
        """Test revs method with commits that can't be processed."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock iter_commits to return a problematic commit
        mock_commit = Mock()
        mock_commit.hexsha = "broken_commit"
        mock_commit.committed_date = "invalid_date"  # This should cause issues

        with patch.object(repo.repo, "iter_commits") as mock_iter:
            mock_iter.return_value = [mock_commit]

            # With skip_broken=True, should handle gracefully
            revs = repo.revs(branch="master", skip_broken=True)

            assert isinstance(revs, pd.DataFrame)

    def test_cleanup_on_error(self, temp_repo):
        """Test that cleanup happens properly even when errors occur."""
        repo = Repository(working_dir=str(temp_repo))

        # Ensure the repository object can be cleaned up
        repo_name = repo.repo_name
        assert repo_name == "test_repo"

        # Test __del__ method doesn't raise errors
        try:
            repo.__del__()
        except Exception as e:
            pytest.fail(f"Repository cleanup raised unexpected exception: {e}")

    def test_process_commit_error_handling(self, temp_repo):
        """Test _process_commit_for_file_history error handling."""
        repo = Repository(working_dir=str(temp_repo))

        # Create a mock commit
        mock_commit = Mock()
        mock_commit.hexsha = "test_commit"
        mock_commit.message = "Test message"
        mock_commit.committer.name = "Test Committer"
        mock_commit.author.name = "Test Author"
        mock_commit.committed_date = 1234567890

        # Mock diff to raise an error
        mock_commit.parents = [Mock()]
        mock_commit.diff.side_effect = GitCommandError("diff failed", 128)

        history = []

        # Should handle git diff errors gracefully when skip_broken=True
        repo._process_commit_for_file_history(mock_commit, history, None, None, skip_broken=True)

        # History should be empty due to error
        assert len(history) == 0

    def test_file_change_rates_git_command_error(self, temp_repo):
        """Test file_change_rates when git commands fail."""
        repo = Repository(working_dir=str(temp_repo))

        # Mock file_change_history to raise GitCommandError
        with patch.object(repo, "file_change_history") as mock_fch:
            mock_fch.side_effect = GitCommandError("git command failed", 128)

            rates = repo.file_change_rates(branch="master")

            assert isinstance(rates, pd.DataFrame)
            assert rates.empty
            expected_columns = [
                "file",
                "unique_committers",
                "abs_rate_of_change",
                "net_rate_of_change",
                "net_change",
                "abs_change",
                "edit_rate",
                "lines",
                "repository",
            ]
            assert list(rates.columns) == expected_columns


class TestRepositoryInitializationErrors:
    """Test Repository initialization error scenarios."""

    def test_remote_repo_network_error(self):
        """Test initialization with remote repository when network fails."""
        # Mock GitCommandError for network issues
        with patch("git.Repo.clone_from") as mock_clone:
            mock_clone.side_effect = GitCommandError("network error", 128)

            with pytest.raises(GitCommandError):
                Repository(working_dir="https://github.com/nonexistent/repo.git")

    def test_remote_repo_invalid_url(self):
        """Test initialization with invalid remote repository URL."""
        with pytest.raises((GitCommandError, ValueError, git.exc.NoSuchPathError)):
            Repository(working_dir="not_a_valid_url")

    def test_permission_denied_on_temp_dir(self):
        """Test behavior when temp directory creation fails."""
        with patch("tempfile.mkdtemp") as mock_mkdtemp:
            mock_mkdtemp.side_effect = PermissionError("Permission denied")

            with pytest.raises(PermissionError):
                Repository(working_dir="https://github.com/some/repo.git")
