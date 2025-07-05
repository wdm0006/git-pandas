import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from git import Repo

from gitpandas import ProjectDirectory, Repository
from gitpandas.project import _has_joblib


@pytest.fixture
def repo_directories(tmp_path):
    """Create multiple test repositories for ProjectDirectory testing."""
    # Create base directory for repos
    repos_dir = tmp_path / "multiple_repos"
    repos_dir.mkdir()

    # Create first repository
    repo1_path = repos_dir / "repo1"
    repo1_path.mkdir()
    repo1 = Repo.init(repo1_path)
    repo1.config_writer().set_value("user", "name", "User1").release()
    repo1.config_writer().set_value("user", "email", "user1@example.com").release()
    repo1.git.checkout("-b", "master")
    (repo1_path / "README.md").write_text("# Repo 1")
    repo1.index.add(["README.md"])
    repo1.index.commit("Initial commit in repo1")

    # Create second repository
    repo2_path = repos_dir / "repo2"
    repo2_path.mkdir()
    repo2 = Repo.init(repo2_path)
    repo2.config_writer().set_value("user", "name", "User2").release()
    repo2.config_writer().set_value("user", "email", "user2@example.com").release()
    repo2.git.checkout("-b", "master")
    (repo2_path / "README.md").write_text("# Repo 2")
    repo2.index.add(["README.md"])
    repo2.index.commit("Initial commit in repo2")

    # Return the directory containing both repos
    return repos_dir


class TestProjectDirectoryConstruction:
    def test_init_with_directory(self, repo_directories):
        """Test initializing ProjectDirectory with a directory path."""
        pd_obj = ProjectDirectory(working_dir=str(repo_directories))

        # Should find both repos
        assert len(pd_obj.repos) == 2
        repo_names = {r.repo_name for r in pd_obj.repos}
        assert "repo1" in repo_names
        assert "repo2" in repo_names

    def test_init_with_explicit_repos(self, repo_directories):
        """Test initializing ProjectDirectory with explicit repository paths."""
        repo_paths = [str(repo_directories / "repo1"), str(repo_directories / "repo2")]
        pd_obj = ProjectDirectory(working_dir=repo_paths)

        # Should use exactly the repos we specified
        assert len(pd_obj.repos) == 2
        repo_names = {r.repo_name for r in pd_obj.repos}
        assert "repo1" in repo_names
        assert "repo2" in repo_names

    def test_init_with_repository_instances(self):
        """Test initializing ProjectDirectory with Repository instances."""
        # Create mock Repository instances
        repo1 = MagicMock(spec=Repository)
        repo1.repo_name = "mock_repo1"

        repo2 = MagicMock(spec=Repository)
        repo2.repo_name = "mock_repo2"

        pd_obj = ProjectDirectory(working_dir=[repo1, repo2])

        # Should use the Repository instances directly
        assert pd_obj.repos == [repo1, repo2]

    def test_init_with_ignore_repos(self, repo_directories):
        """Test initializing ProjectDirectory with ignore_repos parameter."""
        pd_obj = ProjectDirectory(working_dir=str(repo_directories), ignore_repos=["repo1"])

        # Should only include repo2
        assert len(pd_obj.repos) == 1
        assert pd_obj.repos[0].repo_name == "repo2"


class TestProjectDirectoryAdvanced:
    def test_hours_estimate(self, repo_directories):
        """Test hours estimation across multiple repositories."""
        pd_obj = ProjectDirectory(working_dir=str(repo_directories))

        # Create a mock method response for all repos
        mock_hours_df = pd.DataFrame(
            {
                "hours": [1.5, 2.0],
                "date": [datetime.datetime.now(), datetime.datetime.now()],
                "repository": ["repo1", "repo2"],
            }
        )

        # Mock the hours_estimate method on all repositories
        for repo in pd_obj.repos:
            repo.hours_estimate = MagicMock(return_value=mock_hours_df[mock_hours_df["repository"] == repo.repo_name])

        # Call hours_estimate and verify results
        result = pd_obj.hours_estimate(branch="master")

        # Should combine the results from both repositories
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result["repository"].unique()) == {"repo1", "repo2"}

        # Verify parameters were passed down to the repository method
        for repo in pd_obj.repos:
            repo.hours_estimate.assert_called_once_with(
                branch="master",
                grouping_window=0.5,
                single_commit_hours=0.5,
                limit=None,
                days=None,
                committer=True,
                ignore_globs=None,
                include_globs=None,
            )

    def test_bus_factor(self, repo_directories):
        """Test bus factor calculation across multiple repositories."""
        pd_obj = ProjectDirectory(working_dir=str(repo_directories))

        # Create mock bus factor responses for individual repos
        mock_repo1_df = pd.DataFrame(
            {"repository": ["repo1"], "total_committers": [2], "total_commits": [10], "bus factor": [1]}
        )

        mock_repo2_df = pd.DataFrame(
            {"repository": ["repo2"], "total_committers": [3], "total_commits": [15], "bus factor": [2]}
        )

        # Mock the bus_factor method on all repositories
        for repo in pd_obj.repos:
            if repo.repo_name == "repo1":
                repo.bus_factor = MagicMock(return_value=mock_repo1_df)
            else:
                repo.bus_factor = MagicMock(return_value=mock_repo2_df)

        # Test with by="repository"
        result = pd_obj.bus_factor(by="repository")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result["repository"].unique()) == {"repo1", "repo2"}
        assert result[result["repository"] == "repo1"]["bus factor"].values[0] == 1
        assert result[result["repository"] == "repo2"]["bus factor"].values[0] == 2

        # Test with by="projectd"
        # Reset mocks
        for repo in pd_obj.repos:
            repo.bus_factor.reset_mock()
            if repo.repo_name == "repo1":
                repo.bus_factor = MagicMock(return_value=mock_repo1_df)
            else:
                repo.bus_factor = MagicMock(return_value=mock_repo2_df)

        result = pd_obj.bus_factor(by="projectd")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1  # Aggregated result
        assert result["bus factor"].values[0] > 0  # Should calculate some bus factor

        # Test with by="file"
        # Create mock file-wise bus factor responses
        mock_repo1_file_df = pd.DataFrame(
            {"file": ["file1.py", "file2.py"], "bus factor": [1, 2], "repository": ["repo1", "repo1"]}
        )

        mock_repo2_file_df = pd.DataFrame(
            {"file": ["file3.py", "file4.py"], "bus factor": [1, 1], "repository": ["repo2", "repo2"]}
        )

        # Reset mocks for file test
        for repo in pd_obj.repos:
            repo.bus_factor.reset_mock()
            if repo.repo_name == "repo1":
                repo.bus_factor = MagicMock(return_value=mock_repo1_file_df)
            else:
                repo.bus_factor = MagicMock(return_value=mock_repo2_file_df)

        result = pd_obj.bus_factor(by="file")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # All files from both repos
        assert set(result["repository"].unique()) == {"repo1", "repo2"}
        assert set(result["file"].unique()) == {"file1.py", "file2.py", "file3.py", "file4.py"}

        # Verify all bus factors are reasonable (at least 1)
        assert (result["bus factor"] >= 1).all()

        # Verify the method was called with the correct parameters
        for repo in pd_obj.repos:
            repo.bus_factor.assert_called_once_with(ignore_globs=None, include_globs=None, by="file")

    def test_file_detail(self, repo_directories):
        """Test file detail retrieval across multiple repositories."""
        pd_obj = ProjectDirectory(working_dir=str(repo_directories))

        # Create mock file detail responses
        mock_repo1_df = pd.DataFrame(
            {"file": ["file1.py", "file2.py"], "loc": [100, 200], "repository": ["repo1", "repo1"]}
        )

        mock_repo2_df = pd.DataFrame(
            {"file": ["file3.py", "file4.py"], "loc": [150, 250], "repository": ["repo2", "repo2"]}
        )

        # Mock the file_detail method on repositories
        for repo in pd_obj.repos:
            if repo.repo_name == "repo1":
                repo.file_detail = MagicMock(return_value=mock_repo1_df)
            else:
                repo.file_detail = MagicMock(return_value=mock_repo2_df)

        # Call file_detail
        result = pd_obj.file_detail(rev="HEAD")

        # Verify results are combined
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # All files from both repos
        # Get unique repositories from the index
        assert set(result.index.get_level_values("repository").unique()) == {"repo1", "repo2"}
        # Get unique files from the index
        assert set(result.index.get_level_values("file").unique()) == {"file1.py", "file2.py", "file3.py", "file4.py"}

        # Verify parameters were passed down correctly
        for repo in pd_obj.repos:
            repo.file_detail.assert_called_once_with(rev="HEAD", committer=True, ignore_globs=None, include_globs=None)


class TestProjectDirectoryParallel:
    @pytest.mark.skipif(not _has_joblib, reason="joblib not available")
    @patch("gitpandas.project._has_joblib", True)
    @patch("gitpandas.project.Parallel")
    @patch("gitpandas.project.delayed")
    def test_revs_parallel(self, mock_delayed, mock_parallel, repo_directories):
        """Test that revs uses parallel processing when joblib is available."""
        pd_obj = ProjectDirectory(working_dir=str(repo_directories))

        # Create mock return values
        mock_result1 = pd.DataFrame({"rev": ["abc123"], "repository": ["repo1"]})
        mock_result2 = pd.DataFrame({"rev": ["def456"], "repository": ["repo2"]})
        mock_parallel_instance = mock_parallel.return_value
        mock_parallel_instance.return_value = [mock_result1, mock_result2]

        # Call revs
        result = pd_obj.revs(branch="master", limit=10)

        # Verify parallel processing was used
        assert mock_parallel.called
        assert mock_delayed.called

        # Verify result combines data from both repos
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result["repository"].unique()) == {"repo1", "repo2"}
