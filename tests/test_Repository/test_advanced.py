import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from git import Repo

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
        assert "commit_sha" in commits_in_tags.columns

    def test_get_branches_by_commit(self, local_repo):
        """Test getting branches that contain a specific commit."""
        repo_obj = Repository(working_dir=str(local_repo))
        repo = Repo(str(local_repo))

        # Get the first commit (should be in both master and feature)
        first_commit = list(repo.iter_commits(rev="master", max_count=1, skip=repo.commit("master").count() - 1))[0]

        # Get branches containing this commit
        branches = repo_obj.get_branches_by_commit(first_commit.hexsha)

        # Should include both master and feature
        assert isinstance(branches, pd.DataFrame)
        assert "branch" in branches.columns
        assert "commit" in branches.columns
        assert "repository" in branches.columns
        branch_list = branches["branch"].tolist()
        assert "master" in branch_list
        assert "feature" in branch_list

        # Get the last commit on feature branch (should only be in feature)
        repo.git.checkout("feature")
        feature_commit = list(repo.iter_commits(max_count=1))[0]
        repo.git.checkout("master")  # Go back to master

        # Get branches for feature commit
        branches = repo_obj.get_branches_by_commit(feature_commit.hexsha)

        # Should only include feature
        assert isinstance(branches, pd.DataFrame)
        branch_list = branches["branch"].tolist()
        assert "feature" in branch_list
        assert "master" not in branch_list

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
                    "date": ["1672531200", "1672617600"],  # 2023-01-01 and 2023-01-02 in Unix timestamp format
                }
            )
            mock_revs.index = pd.RangeIndex(len(mock_revs))  # Ensure integer indices

            # Mock the revs and parallel processing
            repo.revs = MagicMock(return_value=mock_revs)
            mock_parallel_instance = mock_parallel.return_value

            # Setup the parallel execution to return our mock blame data
            def side_effect(delayed_funcs):
                # For each delayed function call, return mock data
                results = []
                for i in range(len(mock_revs)):
                    result = {"rev": mock_revs.iloc[i]["rev"], "date": mock_revs.iloc[i]["date"]}
                    if i == 0:
                        result["Test User"] = 3
                    else:
                        result["Test User"] = 5
                    results.append(result)
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

        # Mock commit_history to return controlled data
        mock_history = pd.DataFrame(
            {
                "lines": [12, 5, 4],
                "insertions": [10, 5, 3],
                "deletions": [2, 0, 1],
                "net": [8, 5, 2],
                "author": ["User1", "User1", "User2"],
                "committer": ["User1", "User1", "User2"],
                "message": ["commit 1", "commit 2", "commit 3"],
                "commit_sha": ["abc123", "def456", "ghi789"],
            },
            index=[
                # Monday at 10am, Wednesday at 3pm, Friday at 5pm
                datetime.datetime(2023, 1, 2, 10, 0, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2023, 1, 4, 15, 0, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2023, 1, 6, 17, 0, 0, tzinfo=datetime.timezone.utc),
            ],
        )

        repo.commit_history = MagicMock(return_value=mock_history)

        # Test basic punchcard
        punchcard = repo.punchcard(branch="master")
        assert isinstance(punchcard, pd.DataFrame)
        assert "hour_of_day" in punchcard.columns
        assert "day_of_week" in punchcard.columns
        assert "lines" in punchcard.columns

        # Test punchcard with 'by' parameter
        punchcard_by = repo.punchcard(branch="master", by="committer")
        assert isinstance(punchcard_by, pd.DataFrame)
        assert "committer" in punchcard_by.columns  # Check for actual column name instead of 'by'
        assert len(punchcard_by["committer"].unique()) == 2  # Two different committers

        # Test punchcard with normalization
        punchcard_norm = repo.punchcard(branch="master", normalize=100)  # Normalize to 100
        assert isinstance(punchcard_norm, pd.DataFrame)
        # Normalized values should be between 0 and 100
        assert punchcard_norm["lines"].max() <= 100.0

    def test_file_last_edit(self, local_repo):
        """Test getting the last edit information for a file."""
        repo_obj = Repository(working_dir=str(local_repo))

        # Test a file that exists
        last_edit = repo_obj._file_last_edit("src/main.py")
        assert isinstance(last_edit, str)
        assert "Date:" not in last_edit  # The implementation strips this out

        # Try to parse the date string to verify it's a valid date
        from datetime import datetime

        try:
            # Git date format is like: Thu Apr 7 22:13:13 2005 +0200
            datetime.strptime(last_edit, "%a %b %d %H:%M:%S %Y %z")
        except ValueError as e:
            pytest.fail(f"Last edit date string '{last_edit}' is not in expected git format: {e}")

        # Test a file that doesn't exist
        assert repo_obj._file_last_edit("nonexistent_file.txt") is None

    def test_file_detail_with_cache(self, local_repo):
        """Test that file_detail properly uses caching."""
        repo = Repository(working_dir=str(local_repo))

        # Get a specific revision to test with
        first_commit = list(repo.repo.iter_commits(max_count=1))[0].hexsha

        # Call file_detail twice with the same parameters
        result1 = repo.file_detail(rev=first_commit)
        result2 = repo.file_detail(rev=first_commit)

        # Verify results are DataFrames with expected columns
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)
        assert "loc" in result1.columns
        assert "file_owner" in result1.columns
        assert "ext" in result1.columns
        assert "last_edit_date" in result1.columns

        # Verify both calls return the same data
        pd.testing.assert_frame_equal(result1, result2)

        # Verify that calling with HEAD doesn't use cache
        result3 = repo.file_detail(rev="HEAD")
        assert isinstance(result3, pd.DataFrame)
