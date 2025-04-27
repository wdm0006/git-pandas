import unittest
from unittest.mock import MagicMock

import git
import pandas as pd
from core.data_fetcher import fetch_overview_data

from gitpandas import Repository


class TestBranchHandling(unittest.TestCase):
    """Test handling of non-existent branches in the application."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock Repository instance
        self.mock_repo = MagicMock(spec=Repository)
        self.mock_repo.repo_name = "test-repo"
        self.mock_repo.default_branch = "main"

        # Setup base commit history DataFrame
        date = pd.Timestamp("2023-01-01", tz="UTC")
        self.commit_df = pd.DataFrame(
            {
                "author": ["Test Author"],
                "committer": ["Test Committer"],
                "message": ["Test commit"],
                "commit_sha": ["abcdef1234567890"],
                "lines": [10],
                "insertions": [5],
                "deletions": [5],
                "net": [0],
                "branch": ["main"],
            },
            index=[date],
        )

        # Setup branches DataFrame
        self.branches_df = pd.DataFrame(
            {"branch": ["main", "develop", "feature/test", "gh-pages"], "local": [True, True, True, False]}
        )

    def test_fetch_overview_with_nonexistent_branch(self):
        """Test fetch_overview_data with a non-existent branch."""
        # Mock branches method to return branches including gh-pages
        self.mock_repo.branches.return_value = self.branches_df

        # Mock commit_history for the default branch to return data
        self.mock_repo.commit_history.side_effect = self._mock_commit_history

        # Mock has_branch to return appropriate values
        self.mock_repo.has_branch.side_effect = self._mock_has_branch

        # Run the fetch_overview_data function
        result = fetch_overview_data(self.mock_repo)

        # Verify that the function completed and returned a valid result
        self.assertIsNotNone(result)
        self.assertIn("data", result)
        self.assertIn("refreshed_at", result)

        # Verify that has_branch was called for each branch
        expected_calls = ["main", "develop", "feature/test", "gh-pages"]
        actual_calls = [args[0] for args, _ in self.mock_repo.has_branch.call_args_list]
        self.assertEqual(sorted(expected_calls), sorted(actual_calls))

        # Verify that commit_history was not called for gh-pages
        commit_history_calls = [
            args[1]
            for args, _ in self.mock_repo.commit_history.call_args_list
            if len(args) > 1 and args[1] == "gh-pages"
        ]
        self.assertEqual(len(commit_history_calls), 0, "commit_history should not be called for non-existent branches")

    def _mock_commit_history(
        self, branch=None, limit=None, days=None, ignore_globs=None, include_globs=None, force_refresh=False
    ):
        """Mock implementation of commit_history."""
        if branch in ["main", "develop"]:
            return self.commit_df
        elif branch == "feature/test":
            # Return an empty DataFrame for feature/test to test that path
            return pd.DataFrame(columns=self.commit_df.columns)
        else:
            # For gh-pages or any other branch, raise GitCommandError
            raise git.exc.GitCommandError(
                "git",
                ["rev-list", "--max-count=1", branch, "--"],
                128,
                b"",
                f"fatal: bad revision '{branch}'\n".encode(),
            )

    def _mock_has_branch(self, branch):
        """Mock implementation of has_branch."""
        return branch in ["main", "develop", "feature/test"]
