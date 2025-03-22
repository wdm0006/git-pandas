import numpy as np
import pandas as pd
import pytest
import warnings
from unittest.mock import MagicMock, patch

from gitpandas.utilities.plotting import plot_cumulative_blame, plot_punchcard, plot_lifeline, HAS_MPL

# Suppress matplotlib warnings about non-interactive backend
warnings.filterwarnings("ignore", category=UserWarning, message="FigureCanvasAgg is non-interactive")

class TestPlotting:
    @pytest.fixture
    def punchcard_data(self):
        """Create sample punchcard data for testing."""
        # Create a DataFrame with sample punchcard data
        data = {
            "hour_of_day": np.repeat(range(24), 7),
            "day_of_week": np.tile(range(7), 24),
            "lines": np.random.randint(1, 100, 24 * 7),
        }
        return pd.DataFrame(data)

    @pytest.fixture
    def punchcard_data_with_by(self):
        """Create sample punchcard data with a 'by' column for testing."""
        # Create a DataFrame with sample punchcard data and a 'by' column
        data = {
            "hour_of_day": np.repeat(range(24), 14),  # 2 values in 'by' column
            "day_of_week": np.tile(range(7), 48),
            "lines": np.random.randint(1, 100, 24 * 7 * 2),
            "by": np.repeat(["repo1", "repo2"], 24 * 7),
        }
        return pd.DataFrame(data)

    @pytest.fixture
    def cumulative_blame_data(self):
        """Create sample cumulative blame data for testing."""
        # Create a DataFrame with sample cumulative blame data
        dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
        data = {
            "date": dates,
            "committer1": np.random.randint(0, 100, len(dates)),
            "committer2": np.random.randint(0, 100, len(dates)),
            "committer3": np.random.randint(0, 100, len(dates)),
        }
        return pd.DataFrame(data).set_index("date")

    def test_plot_punchcard_basic(self, punchcard_data):
        """Test basic punchcard plotting."""
        # Test with default parameters
        plot_punchcard(punchcard_data)

        # Test with custom metric
        plot_punchcard(punchcard_data, metric="lines")

        # Test with custom title
        plot_punchcard(punchcard_data, title="Custom Title")

    def test_plot_punchcard_with_by(self, punchcard_data_with_by):
        """Test punchcard plotting with 'by' parameter."""
        # Test with 'by' parameter
        plot_punchcard(punchcard_data_with_by, by="by")

        # Test with 'by' parameter and custom title
        plot_punchcard(punchcard_data_with_by, by="by", title="Custom Title")

    def test_plot_punchcard_no_matplotlib(self, punchcard_data, monkeypatch):
        """Test punchcard plotting when matplotlib is not available."""
        # Mock matplotlib import to simulate it not being available
        monkeypatch.setattr("gitpandas.utilities.plotting.HAS_MPL", False)

        # Should raise ImportError
        with pytest.raises(ImportError):
            plot_punchcard(punchcard_data)

    def test_plot_cumulative_blame(self, cumulative_blame_data):
        """Test cumulative blame plotting."""
        # Test with default parameters
        plot_cumulative_blame(cumulative_blame_data)

    def test_plot_cumulative_blame_no_matplotlib(self, cumulative_blame_data, monkeypatch):
        """Test cumulative blame plotting when matplotlib is not available."""
        # Mock matplotlib import to simulate it not being available
        monkeypatch.setattr("gitpandas.utilities.plotting.HAS_MPL", False)

        # Should raise ImportError
        with pytest.raises(ImportError):
            plot_cumulative_blame(cumulative_blame_data)

    def test_plot_lifeline_basic(self):
        """Test the plot_lifeline function with basic input."""
        if not HAS_MPL:
            pytest.skip("matplotlib is not installed")
        
        # Mock matplotlib functions to avoid display issues
        with patch('matplotlib.pyplot.show'), patch('matplotlib.pyplot.subplots', return_value=(MagicMock(), MagicMock())):
            # Create test data
            dates = pd.date_range(start='2023-01-01', periods=10)
            
            # File change history
            changes = pd.DataFrame({
                'filename': ['file1.py', 'file2.py', 'file1.py', 'file3.py'],
                'author': ['User1', 'User1', 'User2', 'User1'],
                'committer': ['User1', 'User1', 'User2', 'User1'],
                'insertions': [10, 5, 8, 12],
                'deletions': [2, 0, 3, 0],
            }, index=dates[:4])
            
            # Ownership changes
            ownership_changes = pd.DataFrame({
                'filename': ['file1.py'],
                'author': ['User2'],
                'committer': ['User2'],
            }, index=[dates[2]])
            
            # Refactoring events
            refactoring = pd.DataFrame({
                'filename': ['file2.py'],
                'message': ['Refactored code'],
            }, index=[dates[1]])
            
            # Call the function
            fig = plot_lifeline(changes, ownership_changes, refactoring)
            
            # Assert that the figure was created
            assert fig is not None

    def test_plot_lifeline_empty_events(self):
        """Test plot_lifeline with empty ownership changes and refactoring."""
        if not HAS_MPL:
            pytest.skip("matplotlib is not installed")
        
        # Create test data with empty events
        dates = pd.date_range(start='2023-01-01', periods=5)
        
        # File change history
        changes = pd.DataFrame({
            'filename': ['file1.py', 'file2.py', 'file1.py'],
            'author': ['User1', 'User1', 'User1'],
            'committer': ['User1', 'User1', 'User1'],
        }, index=dates[:3])
        
        # Empty ownership changes and refactoring
        ownership_changes = pd.DataFrame({
            'filename': [],
            'author': [],
            'committer': [],
        })
        
        refactoring = pd.DataFrame({
            'filename': [],
            'message': [],
        })
        
        # Set a non-interactive backend
        import matplotlib
        matplotlib.use('Agg')
        
        # Call the function
        fig = plot_lifeline(changes, ownership_changes, refactoring)
        
        # Assert that the figure was created
        assert fig is not None

    def test_plot_lifeline_no_matplotlib(self, monkeypatch):
        """Test that plot_lifeline raises ImportError when matplotlib is not available."""
        # Temporarily patch HAS_MPL to simulate matplotlib not being available
        monkeypatch.setattr('gitpandas.utilities.plotting.HAS_MPL', False)
        
        # Create dummy data
        dates = pd.date_range(start='2023-01-01', periods=3)
        changes = pd.DataFrame({'filename': ['file1.py']}, index=[dates[0]])
        ownership_changes = pd.DataFrame({'filename': []})
        refactoring = pd.DataFrame({'filename': []})
        
        # Check that the function raises the expected exception
        with pytest.raises(ImportError):
            plot_lifeline(changes, ownership_changes, refactoring)
