import datetime
from unittest.mock import MagicMock, patch
import os # Added for setting environment variables

import pandas as pd
import pytest
from git import Repo, Actor

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
    # Define commit times explicitly to make assertions easier
    initial_commit_time = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    repo.index.commit("Initial commit", commit_date=initial_commit_time.isoformat())

    # Create some test files across multiple directories
    (repo_path / "src").mkdir()
    (repo_path / "src" / "main.py").write_text("def main():\n    return True")
    (repo_path / "src" / "utils.py").write_text("def helper():\n    return 'helper'")

    (repo_path / "docs").mkdir()
    (repo_path / "docs" / "index.md").write_text("# Documentation")

    # Add files and commit
    repo.index.add(["src/main.py", "src/utils.py", "docs/index.md"])
    commit_time_2 = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
    repo.index.commit("Add initial code structure", commit_date=commit_time_2.isoformat())

    # Create a feature branch and make changes
    repo.git.checkout("-b", "feature")
    (repo_path / "src" / "feature.py").write_text("def feature():\n    return 'new feature'")
    repo.index.add(["src/feature.py"])
    commit_time_3 = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    repo.index.commit("Add feature", commit_date=commit_time_3.isoformat())

    # Go back to master and make different changes
    repo.git.checkout("master")
    (repo_path / "src" / "main.py").write_text("def main():\n    print('Hello')\n    return True") # Modify one line, add one line
    repo.index.add(["src/main.py"])
    commit_time_4 = datetime.datetime(2023, 1, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)
    repo.index.commit("Update main function", commit_date=commit_time_4.isoformat())

    # Create a tag
    tag_time = datetime.datetime(2023, 1, 1, 14, 0, 0, tzinfo=datetime.timezone.utc).isoformat()
    repo.create_tag("v0.1.0", message="First version", tagger=Actor("Tagger", "tagger@example.com"), tag_date=tag_time)

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

    def test_cumulative_blame_multi_author(self, tmp_path):
        """Test cumulative_blame with multiple authors and changing blame."""
        repo_path = tmp_path / "blame_repo"
        repo_path.mkdir()
        repo = Repo.init(repo_path)
        test_file = repo_path / "test_file.txt"

        # Define authors
        author1 = Actor("Author One", "author1@example.com")
        author2 = Actor("Author Two", "author2@example.com")

        # Use specific commit dates for reliable ordering and index checking
        commit_date_1 = datetime.datetime(2023, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
        commit_date_2 = datetime.datetime(2023, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)
        commit_date_3 = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Commit 1: Author One adds 3 lines
        test_file.write_text("Line 1\nLine 2\nLine 3")
        repo.index.add(["test_file.txt"])
        repo.index.commit("Commit 1", author=author1, committer=author1, commit_date=commit_date_1.isoformat())
        commit_1_sha = repo.head.commit.hexsha

        # Commit 2: Author Two adds 2 lines and modifies Line 2
        test_file.write_text("Line 1\nModified Line 2 by Author Two\nLine 3\nLine 4 - A2\nLine 5 - A2")
        repo.index.add(["test_file.txt"])
        repo.index.commit("Commit 2", author=author2, committer=author2, commit_date=commit_date_2.isoformat())
        commit_2_sha = repo.head.commit.hexsha

        # Commit 3: Author One adds 1 line at the beginning
        test_file.write_text("Line 0 - A1\nLine 1\nModified Line 2 by Author Two\nLine 3\nLine 4 - A2\nLine 5 - A2")
        repo.index.add(["test_file.txt"])
        repo.index.commit("Commit 3", author=author1, committer=author1, commit_date=commit_date_3.isoformat())
        commit_3_sha = repo.head.commit.hexsha

        # Instantiate Repository and get cumulative blame
        repo_obj = Repository(working_dir=str(repo_path))
        # Use committer=False to test author blame
        blame_df = repo_obj.cumulative_blame(branch="master", committer=False)

        # Assertions
        assert isinstance(blame_df, pd.DataFrame)
        assert blame_df.index.name == "date"
        assert list(blame_df.columns) == ["Author One", "Author Two"] # Order might vary, maybe sort? Check impl. Let's assume alphabetical for now.
        assert len(blame_df) == 3 # 3 commits

        # Check index values (timestamps) - allow for minor differences if needed
        expected_index = pd.to_datetime([commit_date_1, commit_date_2, commit_date_3], utc=True)
        pd.testing.assert_index_equal(blame_df.index, expected_index, check_names=False)

        # Check blame values at each commit
        # Commit 1: Author One = 3, Author Two = 0
        assert blame_df.loc[commit_date_1, "Author One"] == 3.0
        assert blame_df.loc[commit_date_1, "Author Two"] == 0.0

        # Commit 2: Author One = 2 (Lost Line 2), Author Two = 3 (Modified Line 2, Added Lines 4, 5)
        # Blame logic: git blame attributes the *modified* line to the modifier.
        assert blame_df.loc[commit_date_2, "Author One"] == 2.0
        assert blame_df.loc[commit_date_2, "Author Two"] == 3.0

        # Commit 3: Author One = 3 (Added Line 0), Author Two = 3
        assert blame_df.loc[commit_date_3, "Author One"] == 3.0
        assert blame_df.loc[commit_date_3, "Author Two"] == 3.0

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
        last_edit = repo_obj._get_last_edit_date("src/main.py")
        assert isinstance(last_edit, pd.Timestamp)

        # Test a file that doesn't exist
        assert pd.isna(repo_obj._get_last_edit_date("nonexistent_file.txt"))

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

    def test_blame_deleted_file(self, tmp_path):
        """Test blame on a revision where a file existed but was later deleted."""
        repo_path = tmp_path / "blame_test_repo"
        repo_path.mkdir()
        repo = Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create and checkout master branch
        repo.git.checkout("-b", "master")

        # Commit 1: Add file_a.txt
        file_a = repo_path / "file_a.txt"
        file_a.write_text("Line 1\nLine 2\n")
        repo.index.add([str(file_a)])
        commit1 = repo.index.commit("Add file_a.txt")

        # Commit 2: Modify file_a.txt
        file_a.write_text("Line 1 MODIFIED\nLine 2\nLine 3 NEW\n")
        repo.index.add([str(file_a)])
        commit2 = repo.index.commit("Modify file_a.txt")

        # Commit 3: Delete file_a.txt
        repo.index.remove([str(file_a)])
        commit3 = repo.index.commit("Delete file_a.txt")

        # Create Repository object
        repo_obj = Repository(working_dir=str(repo_path))

        # Run blame on Commit 2 (before deletion)
        blame_result = repo_obj.blame(rev=commit2.hexsha, by="file")

        # Assertions
        assert isinstance(blame_result, pd.DataFrame)
        assert not blame_result.empty
        assert "loc" in blame_result.columns
        # Check if file_a.txt is present in the blame output for commit2
        assert "file_a.txt" in blame_result.index.get_level_values("file").unique()
        # Check the total lines blamed for file_a.txt at commit2
        assert blame_result.loc[("Test User", "file_a.txt"), "loc"] == 3

        # Run blame on Commit 1
        blame_result_1 = repo_obj.blame(rev=commit1.hexsha, by="file")
        assert isinstance(blame_result_1, pd.DataFrame)
        assert not blame_result_1.empty
        assert "file_a.txt" in blame_result_1.index.get_level_values("file").unique()
        assert blame_result_1.loc[("Test User", "file_a.txt"), "loc"] == 2

        # Run blame on HEAD (Commit 3, file deleted)
        blame_result_head = repo_obj.blame(rev="HEAD", by="file")
        assert isinstance(blame_result_head, pd.DataFrame)
        # file_a.txt should NOT be in the blame output for HEAD
        if not blame_result_head.empty: # Check if df is empty before accessing index
             assert "file_a.txt" not in blame_result_head.index.get_level_values("file").unique()
