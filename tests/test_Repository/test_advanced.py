import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from git import Commit, Repo

from gitpandas import Repository


@pytest.fixture
def local_repo(tmp_path):
    """Create a more complex local git repository for advanced testing."""
    repo_path = tmp_path / "advanced_repo"
    repo_path.mkdir()
    repo = Repo.init(repo_path)

    # Configure git user
    repo.config_writer().set_value("user", "name", "Test User").release()
    repo.config_writer().set_value("user", "email", "test@example.com").release()

    # Create and checkout master branch
    repo.git.checkout("-b", "master")

    # Create an initial commit
    (repo_path / "README.md").write_text("# Advanced Test Repository")
    repo.index.add(["README.md"])
    repo.index.commit("Initial commit")

    # Create some test files across multiple directories
    (repo_path / "src").mkdir()
    (repo_path / "src" / "main.py").write_text("def main():\n    return True")
    (repo_path / "src" / "utils.py").write_text("def helper():\n    return 'helper'")

    (repo_path / "docs").mkdir()
    (repo_path / "docs" / "index.md").write_text("# Documentation")

    # Add files and commit
    repo.index.add(["src/main.py", "src/utils.py", "docs/index.md"])
    repo.index.commit("Add initial code structure")

    # Create a feature branch and make changes
    repo.git.checkout("-b", "feature")
    (repo_path / "src" / "feature.py").write_text("def feature():\n    return 'new feature'")
    repo.index.add(["src/feature.py"])
    repo.index.commit("Add feature")

    # Go back to master and make different changes
    repo.git.checkout("master")
    (repo_path / "src" / "main.py").write_text("def main():\n    print('Hello')\n    return True")
    repo.index.add(["src/main.py"])
    repo.index.commit("Update main function")

    # Create a tag
    repo.create_tag("v0.1.0", message="First version")

    return repo_path


class TestRepositoryAdvanced:
    def test_commits_in_tags(self, local_repo):
        """Test retrieving commits between tags."""
        repo = Repository(working_dir=str(local_repo))

        # Get the result
        commits_in_tags = repo.commits_in_tags()

        # Should contain data about commits in the tagged range
        assert isinstance(commits_in_tags, pd.DataFrame)
        assert "tag" in commits_in_tags.columns
        assert "commits" in commits_in_tags.columns

    def test_get_branches_by_commit(self, local_repo):
        """Test getting branches that contain a specific commit."""
        repo_obj = Repository(working_dir=str(local_repo))
        repo = Repo(str(local_repo))

        # Get the first commit (should be in both master and feature)
        first_commit = list(repo.iter_commits(max_count=1, rev="master"))[0]

        # Get branches containing this commit
        branches = repo_obj.get_branches_by_commit(first_commit.hexsha)

        # Should include both master and feature
        assert isinstance(branches, list)
        assert "master" in branches
        assert "feature" in branches

        # Get the last commit on feature branch (should only be in feature)
        repo.git.checkout("feature")
        feature_commit = list(repo.iter_commits(max_count=1))[0]
        repo.git.checkout("master")  # Go back to master

        # Get branches for feature commit
        branches = repo_obj.get_branches_by_commit(feature_commit.hexsha)

        # Should only include feature
        assert isinstance(branches, list)
        assert "feature" in branches
        assert "master" not in branches

    def test_file_owner(self, local_repo):
        """Test getting the owner of a file as of a specific revision."""
        repo = Repository(working_dir=str(local_repo))

        # Get the file owner information
        owner_info = repo.file_owner(rev="HEAD", filename="src/main.py", committer=True)

        # Should return owner information
        assert isinstance(owner_info, dict)
        assert "name" in owner_info
        assert owner_info["name"] == "Test User"

    def test_parallel_cumulative_blame(self, local_repo):
        """Test parallel cumulative blame calculation."""
        repo = Repository(working_dir=str(local_repo))

        # We need to patch the Parallel function to simulate joblib if not available
        with patch("gitpandas.repository._has_joblib", True), patch("gitpandas.repository.Parallel") as mock_parallel:
            # Setup mock return values
            pd.DataFrame({"loc": {"Test User": 3}})
            pd.DataFrame({"loc": {"Test User": 5}})

            # Create a sequence of revision data with attached blame results
            mock_revs = pd.DataFrame(
                {
                    "rev": ["abc123", "def456"],
                    "date": [datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 2)],
                }
            ).set_index("date")

            # Mock the revs and parallel processing
            repo.revs = MagicMock(return_value=mock_revs)
            mock_parallel_instance = mock_parallel.return_value

            # Setup the parallel execution to return our mock blame data
            def side_effect(items, n_jobs):
                # For each item in the parallel call, attach the blame data
                results = []
                for i, item in enumerate(items):
                    if i == 0:
                        item.update({"Test User": 3})
                    else:
                        item.update({"Test User": 5})
                    results.append(item)
                return results

            mock_parallel_instance.side_effect = side_effect

            # Call the function
            result = repo.parallel_cumulative_blame(branch="master", workers=2)

            # Verify the result
            assert isinstance(result, pd.DataFrame)
            assert "Test User" in result.columns
            assert len(result) == 2  # Two revisions

            # Verify parallel was called
            assert mock_parallel.called

    def test_punchcard(self, local_repo):
        """Test punchcard generation with various parameters."""
        repo = Repository(working_dir=str(local_repo))

        # Mock file_change_history to return controlled data
        mock_history = pd.DataFrame(
            {
                "filename": ["file1.py", "file2.py", "file1.py"],
                "insertions": [10, 5, 3],
                "deletions": [2, 0, 1],
                "lines": [12, 5, 4],
                "author": ["User1", "User1", "User2"],
                "committer": ["User1", "User1", "User2"],
            },
            index=[
                # Monday at 10am, Wednesday at 3pm, Friday at 5pm
                datetime.datetime(2023, 1, 2, 10, 0, 0),
                datetime.datetime(2023, 1, 4, 15, 0, 0),
                datetime.datetime(2023, 1, 6, 17, 0, 0),
            ],
        )

        repo.file_change_history = MagicMock(return_value=mock_history)

        # Test basic punchcard
        punchcard = repo.punchcard(branch="master")
        assert isinstance(punchcard, pd.DataFrame)
        assert "hour_of_day" in punchcard.columns
        assert "day_of_week" in punchcard.columns
        assert "lines" in punchcard.columns

        # Test punchcard with 'by' parameter
        punchcard_by = repo.punchcard(branch="master", by="committer")
        assert isinstance(punchcard_by, pd.DataFrame)
        assert "by" in punchcard_by.columns
        assert len(punchcard_by["by"].unique()) == 2  # Two different committers

        # Test punchcard with normalization
        punchcard_norm = repo.punchcard(branch="master", normalize="hour")
        assert isinstance(punchcard_norm, pd.DataFrame)
        # Normalized values should be between 0 and 1
        assert punchcard_norm["lines"].max() <= 1.0

    def test_file_last_edit(self, local_repo):
        """Test getting the last edit information for a file."""
        repo_obj = Repository(working_dir=str(local_repo))

        # Test a file that exists
        last_edit = repo_obj._file_last_edit("src/main.py")
        assert isinstance(last_edit, Commit)
        assert last_edit.message.strip() == "Update main function"

        # Test a file that doesn't exist
        with pytest.raises(FileNotFoundError):
            repo_obj._file_last_edit("nonexistent_file.txt")

    @patch("gitpandas.repository.multicache")
    def test_file_detail_with_cache(self, mock_multicache, local_repo):
        """Test that file_detail properly uses the cache decorator."""
        # Setup mock decorator that just calls the original function
        mock_multicache.side_effect = lambda *args, **kwargs: lambda f: f

        repo = Repository(working_dir=str(local_repo))

        # Call file_detail
        result = repo.file_detail(rev="HEAD")

        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert "file" in result.columns
        assert "loc" in result.columns
        assert "last_edit_date" in result.columns

        # Verify multicache was called with correct parameters
        mock_multicache.assert_called_with(
            key_prefix="file_detail",
            key_list=["include_globs", "ignore_globs", "rev", "committer"],
            skip_if=mock_multicache.call_args[1]["skip_if"],
        )
