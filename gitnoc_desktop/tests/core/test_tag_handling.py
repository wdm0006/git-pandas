import unittest
from unittest.mock import MagicMock

import git
import pandas as pd
from core.data_fetcher import fetch_tags_data

from gitpandas import Repository


class TestTagHandling(unittest.TestCase):
    """Test handling of corrupted tag data."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock Repository instance
        self.mock_repo = MagicMock(spec=Repository)
        self.mock_repo.repo_name = "test-repo"

    def test_fetch_tags_data_with_corrupted_tags(self):
        """Test fetching tags data with corrupted tags."""
        # Mock the tags method to simulate errors
        self.mock_repo.tags.side_effect = self._mock_tags_method

        # Run the fetch_tags_data function
        result = fetch_tags_data(self.mock_repo)

        # Verify that the function completed and returned a valid result
        self.assertIsNotNone(result)
        self.assertIn("data", result)
        self.assertIn("refreshed_at", result)

        # The data should contain a tags dataframe, even if it's empty or minimal
        self.assertIn("tags", result["data"])

        # Check different ways of handling error - we now expect 2 calls with GitCommandError handling
        self.assertEqual(self.mock_repo.tags.call_count, 2)
        call_args_list = self.mock_repo.tags.call_args_list

        # 1st call - regular with no skip_broken
        self.assertEqual(call_args_list[0][1].get("force_refresh", False), False)
        self.assertNotIn("skip_broken", call_args_list[0][1])

        # 2nd call - with skip_broken=True after GitCommandError
        self.assertTrue(call_args_list[1][1].get("force_refresh", True))
        self.assertTrue(call_args_list[1][1].get("skip_broken", False))

    def test_fetch_tags_data_with_file_handle_error(self):
        """Test fetching tags data with file handle error."""
        # Mock the tags method to simulate read of closed file error
        self.mock_repo.tags.side_effect = self._mock_tags_file_error

        # Run the fetch_tags_data function
        result = fetch_tags_data(self.mock_repo)

        # Verify that the function completed and returned a valid result
        self.assertIsNotNone(result)
        self.assertIn("data", result)
        self.assertIn("refreshed_at", result)

        # The data should contain a tags dataframe, even if it's empty or minimal
        self.assertIn("tags", result["data"])

        # Check different ways of handling error - we expect 2 calls with ValueError handling
        self.assertEqual(self.mock_repo.tags.call_count, 2)
        call_args_list = self.mock_repo.tags.call_args_list

        # 1st call - regular with no skip_broken
        self.assertEqual(call_args_list[0][1].get("force_refresh", False), False)
        self.assertNotIn("skip_broken", call_args_list[0][1])

        # 2nd call - with skip_broken=True after ValueError
        self.assertTrue(call_args_list[1][1].get("force_refresh", True))
        self.assertTrue(call_args_list[1][1].get("skip_broken", False))

    def test_tags_unknown_object_type_error(self):
        """Test handling of 'unknown object type' error in tag processing."""
        # Create a mock Repository instance with specific tag behavior
        mock_repo = MagicMock(spec=Repository)
        mock_repo.repo_name = "test-repo"

        # Set up the tags method to first raise the error, then succeed with skip_broken
        def side_effect_for_tags(*args, **kwargs):
            if "skip_broken" in kwargs and kwargs["skip_broken"]:
                # Return a valid DataFrame when called with skip_broken=True
                date = pd.Timestamp("2023-01-01", tz="UTC")
                df = pd.DataFrame(
                    {
                        "tag_date": [date],
                        "commit_date": [date],
                        "tag": ["v1.0.0"],
                        "annotated": [True],
                        "annotation": ["Tag 1"],
                        "tag_sha": ["abc123"],
                        "commit_sha": ["def456"],
                    }
                )
                df = df.set_index(keys=["tag_date", "commit_date"], drop=True)
                return df
            else:
                # Simulate the specific error from the logs
                raise git.exc.GitCommandError(["git", "cat-file", "tag", "v1.2.0"], 128, "error: unknown object type")

        mock_repo.tags.side_effect = side_effect_for_tags

        # Run the fetch_tags_data function
        result = fetch_tags_data(mock_repo)

        # Verify result has the expected structure
        self.assertIn("data", result)
        self.assertIn("tags", result["data"])
        self.assertIsNotNone(result["data"]["tags"])

        # Should have called tags twice
        self.assertEqual(mock_repo.tags.call_count, 2)

        # First call should be regular
        first_call_kwargs = mock_repo.tags.call_args_list[0][1]
        self.assertEqual(first_call_kwargs.get("force_refresh", False), False)

        # Second call should include skip_broken=True
        second_call_kwargs = mock_repo.tags.call_args_list[1][1]
        self.assertTrue(second_call_kwargs.get("force_refresh", False))
        self.assertTrue(second_call_kwargs.get("skip_broken", False))

        # Verify the result contains the expected data
        tags_df = result["data"]["tags"]
        self.assertEqual(len(tags_df), 1)
        self.assertEqual(tags_df["tag"].iloc[0], "v1.0.0")

    def test_tags_unknown_object_type_double_failure(self):
        """Test handling when both normal fetch and skip_broken retry fail."""
        # Create a mock Repository instance
        mock_repo = MagicMock(spec=Repository)
        mock_repo.repo_name = "test-repo"

        # Make tags method always fail with unknown object type error
        def side_effect_for_tags(*args, **kwargs):
            # Always raise the error regardless of parameters
            raise git.exc.GitCommandError(["git", "cat-file", "tag", "v1.2.0"], 128, "error: unknown object type")

        mock_repo.tags.side_effect = side_effect_for_tags

        # Run the fetch_tags_data function
        result = fetch_tags_data(mock_repo)

        # Verify result has the expected structure
        self.assertIn("data", result)
        self.assertIn("tags", result["data"])

        # Should be a DataFrame with expected columns but empty
        tags_df = result["data"]["tags"]
        self.assertIsInstance(tags_df, pd.DataFrame)

        # Verify columns match what we expect for the fallback DataFrame
        expected_columns = ["tag", "date", "message", "author"]
        self.assertListEqual(sorted(tags_df.columns.tolist()), sorted(expected_columns))

        # Verify tags method was called twice
        self.assertEqual(mock_repo.tags.call_count, 2)

    def _mock_tags_method(self, force_refresh=False, skip_broken=False):
        """Mock implementation of tags method to simulate different error scenarios."""
        if not skip_broken:
            # First call - simulate a GitCommandError with "unknown object type"
            raise git.exc.GitCommandError(["git", "cat-file", "tag", "v1.2.0"], 128, "error: unknown object type")
        else:
            # Second call with skip_broken=True, return a minimal dataframe
            # This simulates successfully skipping problematic tags
            date = pd.Timestamp("2023-01-01", tz="UTC")
            df = pd.DataFrame(
                {
                    "tag_date": [date],
                    "commit_date": [date],
                    "tag": ["v1.0.0"],
                    "annotated": [True],
                    "annotation": ["Tag 1"],
                    "tag_sha": ["abc123"],
                    "commit_sha": ["def456"],
                }
            )
            df = df.set_index(keys=["tag_date", "commit_date"], drop=True)
            return df

    def _mock_tags_file_error(self, force_refresh=False, skip_broken=False):
        """Mock implementation of tags method to simulate file handle errors."""
        if not skip_broken:
            # First call - simulate a ValueError with "read of closed file"
            raise ValueError("read of closed file")
        else:
            # Second call with skip_broken=True, return a minimal dataframe
            date = pd.Timestamp("2023-01-01", tz="UTC")
            df = pd.DataFrame(
                {
                    "tag_date": [date],
                    "commit_date": [date],
                    "tag": ["v1.0.0"],
                    "annotated": [True],
                    "annotation": ["Tag 1"],
                    "tag_sha": ["abc123"],
                    "commit_sha": ["def456"],
                }
            )
            df = df.set_index(keys=["tag_date", "commit_date"], drop=True)
            return df


if __name__ == "__main__":
    unittest.main()
