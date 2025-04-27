import unittest
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime, timedelta, timezone
import sys
import os
from pathlib import Path
import git
from gitpandas.cache import CacheMissError

# Add the parent directory to sys.path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Import the function we want to test
from core.data_fetcher import fetch_overview_data, fetch_cumulative_blame_data, fetch_tags_data


class TestDataFetcher(unittest.TestCase):
    
    @patch('core.data_fetcher.logger')
    def test_active_branches_handles_problematic_branch_data(self, mock_logger):
        """Test handling of active branches with problematic branch data."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        mock_repo.default_branch = "main"
        
        # Create a future timestamp to ensure all branches are within cutoff
        future_time = datetime.now(timezone.utc) + timedelta(days=1)
        
        # Create mock data for base_commits - one commit
        mock_base_commits = pd.DataFrame({
            'committer': ['Test User'],
            'author': ['Test User'],
            'message': ['Test commit']
        }, index=[future_time])  # Use timezone aware datetime
        
        # Mock branches - return multiple branches
        mock_branches = pd.DataFrame({
            'branch': ['main', 'feature', 'problematic_branch', 'git_error_branch', 'exception_branch'],
        })
        
        # Branch commit responses
        
        # 1. Empty dataframe that will cause index[0] to fail
        mock_branch_commits_empty = pd.DataFrame({
            'author': []
        }, index=pd.DatetimeIndex([]))
        
        # 2. Normal branch commits that will work
        mock_branch_commits_normal = pd.DataFrame({
            'author': ['Test User']
        }, index=[future_time]) 
        
        # 3. Dataframe missing 'author' column to trigger KeyError
        mock_branch_commits_no_author = pd.DataFrame({
            'committer': ['Another User']  # Missing 'author' column
        }, index=[future_time])
        
        # Git command error and other exceptions will be handled with side_effect logic
        
        # Define side effects for commit_history calls
        def side_effect_for_commit_history(branch=None, limit=None, force_refresh=False):
            if branch == 'main':
                if limit == 1:  # Base commits call
                    return mock_base_commits
                return mock_branch_commits_normal
            elif branch == 'feature':
                return mock_branch_commits_normal
            elif branch == 'problematic_branch':
                return mock_branch_commits_empty
            elif branch == 'git_error_branch':
                raise git.exc.GitCommandError('git', 1, 'git error message')
            elif branch == 'exception_branch':
                raise Exception('Some other error')
            elif branch == mock_repo.default_branch:
                return mock_base_commits
            else:
                return mock_branch_commits_no_author
        
        # Set up the mock
        mock_repo.commit_history.side_effect = side_effect_for_commit_history
        mock_repo.branches.return_value = mock_branches
        
        # Call the function under test
        result = fetch_overview_data(mock_repo)
        
        # Verify result contains active_branches
        self.assertIn('data', result)
        self.assertIn('active_branches', result['data'])
        
        # Access the active_branches dataframe
        active_branches = result['data']['active_branches']
        
        # Basic validations
        self.assertIsInstance(active_branches, pd.DataFrame)
        self.assertIn('branch', active_branches.columns)
        self.assertIn('last_commit_date', active_branches.columns)
        self.assertIn('author', active_branches.columns)
        
        # We should only have branches that successfully passed all checks
        # In our case, that should be 'main' and 'feature' (2 branches)
        self.assertEqual(len(active_branches), 2, 
                        f"Expected 2 active branches, got {len(active_branches)}: {active_branches['branch'].tolist()}")
        
        # Verify both branches are in the result
        branch_names = active_branches['branch'].tolist()
        self.assertIn('main', branch_names)
        self.assertIn('feature', branch_names)
        
        # Verify problematic branches are not included
        self.assertNotIn('problematic_branch', branch_names)
        self.assertNotIn('git_error_branch', branch_names)
        self.assertNotIn('exception_branch', branch_names)
        
        # Verify logging for issues - use a less strict check
        self.assertTrue(any("git_error_branch" in str(args[0]) for args in mock_logger.warning.call_args_list), 
                       "No warning log for git_error_branch found")
        self.assertTrue(any("exception_branch" in str(args[0]) for args in mock_logger.warning.call_args_list),
                       "No warning log for exception_branch found")

    @patch('core.data_fetcher.logger')
    def test_active_branches_empty_list_handling(self, mock_logger):
        """Test handling of active branches when no branches qualify."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        mock_repo.default_branch = "main"
        
        # Create mock data for base_commits
        mock_base_commits = pd.DataFrame({
            'committer': ['Test User'],
            'author': ['Test User'],
            'message': ['Test commit']
        }, index=[datetime.now(timezone.utc)])
        
        # Mock branches - return branches
        mock_branches = pd.DataFrame({
            'branch': ['main', 'feature'],
        })
        
        # Empty dataframe for all branch commits - none will qualify
        mock_branch_commits_empty = pd.DataFrame({
            'author': []
        }, index=pd.DatetimeIndex([]))
        
        # Set up the mocks - all branches return empty commits
        mock_repo.commit_history.side_effect = [
            mock_base_commits,          # First call for base_commits
            mock_branch_commits_empty,   # For main branch
            mock_branch_commits_empty    # For feature branch
        ]
        mock_repo.branches.return_value = mock_branches
        
        # Call the function under test
        result = fetch_overview_data(mock_repo)
        
        # Verify result contains active_branches dataframe with expected columns
        active_branches = result['data']['active_branches']
        self.assertIsInstance(active_branches, pd.DataFrame)
        self.assertEqual(len(active_branches), 0)  # Should be empty
        self.assertIn('branch', active_branches.columns)
        self.assertIn('last_commit_date', active_branches.columns)
        self.assertIn('author', active_branches.columns)

    @patch('core.data_fetcher.logger')
    def test_cumulative_blame_direct_keyerror(self, mock_logger):
        """Test handling of direct KeyError: 'rev' in cumulative_blame method."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        
        # Set up the mock to raise KeyError when cumulative_blame is called
        def side_effect_for_cumulative_blame(*args, **kwargs):
            # Raise KeyError directly to simulate error from gitpandas
            raise KeyError('rev')
            
        mock_repo.cumulative_blame.side_effect = side_effect_for_cumulative_blame
        
        # Call the function under test
        result = fetch_cumulative_blame_data(mock_repo)
        
        # Verify result has the expected structure despite the error
        self.assertIn('data', result)
        self.assertIn('cumulative_blame', result['data'])
        self.assertIsNone(result['data']['cumulative_blame'])
        
        # Verify the error was logged
        mock_logger.exception.assert_called_once()
        # Check for specific error message
        self.assertTrue(any("'rev'" in str(args[0]) for args in mock_logger.exception.call_args_list),
                      "No error log for KeyError: 'rev' found")
    
    @patch('core.data_fetcher.logger')
    def test_cumulative_blame_cache_keyerror(self, mock_logger):
        """Test handling of KeyError: 'rev' during cache operations."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        
        # Set up a successful return first, but problematic cache
        mock_df = pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', periods=3),
            'author1': [10, 20, 30],
            'author2': [5, 10, 15]
        })
        mock_df.index = mock_df['date']  # Set index to dates
        del mock_df['date']  # Remove the date column after setting as index
        
        # Create a mock cache backend that raises KeyError on get
        mock_cache = MagicMock()
        def get_side_effect(key):
            raise KeyError('rev')  # Simulate KeyError during cache operations
            
        mock_cache.get.side_effect = get_side_effect
        
        # Set up property mock for cache_backend to return our mock
        type(mock_repo).cache_backend = PropertyMock(return_value=mock_cache)
        
        # Make the cumulative_blame succeed normally
        mock_repo.cumulative_blame.return_value = mock_df
        
        # Call the function under test
        result = fetch_cumulative_blame_data(mock_repo, force_refresh=False)
        
        # Verify we get a valid result despite cache error
        self.assertIn('data', result)
        self.assertIn('cumulative_blame', result['data'])
        self.assertIsNotNone(result['data']['cumulative_blame'])
        # Should return the DataFrame from the direct call
        pd.testing.assert_frame_equal(result['data']['cumulative_blame'], mock_df)
        
        # For this test, we won't expect an exception because the KeyError from cache
        # is handled by the multicache decorator, not our function directly
        # We're just verifying it properly falls back to direct method call

    @patch('core.data_fetcher.logger')
    def test_cumulative_blame_handles_cache_miss(self, mock_logger):
        """Test handling of cache miss in cumulative_blame method."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        
        # Create a mock DataFrame for the successful function call
        mock_df = pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', periods=3),
            'author1': [10, 20, 30],
            'author2': [5, 10, 15]
        })
        mock_df.index = mock_df['date']  # Set index to dates
        del mock_df['date']  # Remove the date column after setting as index
        
        # Make the direct cumulative_blame call work
        mock_repo.cumulative_blame.return_value = mock_df
        
        # Create a mock cache backend that raises CacheMissError
        mock_cache = MagicMock()
        mock_cache.get.side_effect = CacheMissError("Key not found")
        
        # Set up property mock for cache_backend
        type(mock_repo).cache_backend = PropertyMock(return_value=mock_cache)
        
        # Call the function under test
        result = fetch_cumulative_blame_data(mock_repo)
        
        # Verify we get a valid result despite cache miss
        self.assertIn('data', result)
        self.assertIn('cumulative_blame', result['data'])
        # Use pandas testing utility for DataFrame comparison
        pd.testing.assert_frame_equal(result['data']['cumulative_blame'], mock_df)
        
        # Verify logging 
        mock_logger.exception.assert_not_called()

    @patch('core.data_fetcher.logger')
    def test_tags_unknown_object_type_error(self, mock_logger):
        """Test handling of 'unknown object type' error in tags fetching."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        
        # Set up the tags method to first raise the specific error, then succeed on retry with specific args
        mock_repo.tags = MagicMock()
        
        # First call raises ValueError with unknown object type
        # Second call (with specific parameters) returns a valid dataframe
        # Any other calls still raise the error
        def side_effect_for_tags(*args, **kwargs):
            if kwargs.get('skip_broken') and kwargs.get('force_refresh'):
                # Return a valid DataFrame when called with our specific parameters
                return pd.DataFrame({
                    'tag': ['v1.0', 'v1.1'],
                    'date': [datetime.now(), datetime.now()],
                    'message': ['Tag 1', 'Tag 2'],
                    'author': ['User1', 'User2']
                })
            # Simulate the specific error from the logs
            raise ValueError("Cannot handle unknown object type: 96c247a4a362cf245cc3e9da3de57664a8cb52dc")
            
        mock_repo.tags.side_effect = side_effect_for_tags
        
        # Call the function under test
        result = fetch_tags_data(mock_repo)
        
        # Verify result has the expected structure with valid data from the successful retry
        self.assertIn('data', result)
        self.assertIn('tags', result['data'])
        self.assertIsNotNone(result['data']['tags'])
        
        # Should be a DataFrame with the expected columns and 2 rows
        tags_df = result['data']['tags']
        self.assertIsInstance(tags_df, pd.DataFrame)
        self.assertEqual(len(tags_df), 2)
        self.assertIn('tag', tags_df.columns)
        self.assertIn('date', tags_df.columns)
        self.assertIn('message', tags_df.columns)
        self.assertIn('author', tags_df.columns)
        
        # Verify the error was logged appropriately
        self.assertTrue(any("unknown object type" in str(args[0]) for args in mock_logger.warning.call_args_list),
                       "No warning log for 'unknown object type' found")
        
        # There should be a log showing retry with skip_broken
        self.assertTrue(any("skip_broken=True" in str(args[0]) for args in mock_logger.warning.call_args_list),
                      "No log for retry attempt with skip_broken found")
                      
    @patch('core.data_fetcher.logger')
    def test_tags_unknown_object_type_double_failure(self, mock_logger):
        """Test handling when both normal fetch and skip_broken retry fail."""
        # Create a mock repository
        mock_repo = MagicMock()
        mock_repo.repo_name = "test-repo"
        
        # Make tags method always fail, even with skip_broken
        def side_effect_for_tags(*args, **kwargs):
            # Always raise the error regardless of parameters
            raise ValueError("Cannot handle unknown object type: 96c247a4a362cf245cc3e9da3de57664a8cb52dc")
            
        mock_repo.tags.side_effect = side_effect_for_tags
        
        # Call the function under test
        result = fetch_tags_data(mock_repo)
        
        # Verify result has the expected structure with an empty DataFrame
        self.assertIn('data', result)
        self.assertIn('tags', result['data'])
        
        # Should be a DataFrame with the expected columns but empty
        tags_df = result['data']['tags']
        self.assertIsInstance(tags_df, pd.DataFrame)
        self.assertEqual(len(tags_df), 0)  # Empty
        self.assertIn('tag', tags_df.columns)
        self.assertIn('date', tags_df.columns)
        self.assertIn('message', tags_df.columns)
        self.assertIn('author', tags_df.columns)
        
        # Verify the retry failure was logged
        self.assertTrue(any("Retry for tags with skip_broken also failed" in str(args[0]) for args in mock_logger.warning.call_args_list),
                       "No warning log for skip_broken retry failure found")

if __name__ == '__main__':
    unittest.main() 