import pytest
import pandas as pd
import numpy as np
from gitpandas.utilities.plotting import plot_punchcard, plot_cumulative_blame

class TestPlotting:
    @pytest.fixture
    def punchcard_data(self):
        """Create sample punchcard data for testing."""
        # Create a DataFrame with sample punchcard data
        data = {
            'hour_of_day': np.repeat(range(24), 7),
            'day_of_week': np.tile(range(7), 24),
            'lines': np.random.randint(1, 100, 24 * 7)
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def punchcard_data_with_by(self):
        """Create sample punchcard data with a 'by' column for testing."""
        # Create a DataFrame with sample punchcard data and a 'by' column
        data = {
            'hour_of_day': np.repeat(range(24), 14),  # 2 values in 'by' column
            'day_of_week': np.tile(range(7), 48),
            'lines': np.random.randint(1, 100, 24 * 7 * 2),
            'by': np.repeat(['repo1', 'repo2'], 24 * 7)
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def cumulative_blame_data(self):
        """Create sample cumulative blame data for testing."""
        # Create a DataFrame with sample cumulative blame data
        dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
        data = {
            'date': dates,
            'committer1': np.random.randint(0, 100, len(dates)),
            'committer2': np.random.randint(0, 100, len(dates)),
            'committer3': np.random.randint(0, 100, len(dates))
        }
        return pd.DataFrame(data).set_index('date')
    
    def test_plot_punchcard_basic(self, punchcard_data):
        """Test basic punchcard plotting."""
        # Test with default parameters
        plot_punchcard(punchcard_data)
        
        # Test with custom metric
        plot_punchcard(punchcard_data, metric='lines')
        
        # Test with custom title
        plot_punchcard(punchcard_data, title='Custom Title')
    
    def test_plot_punchcard_with_by(self, punchcard_data_with_by):
        """Test punchcard plotting with 'by' parameter."""
        # Test with 'by' parameter
        plot_punchcard(punchcard_data_with_by, by='by')
        
        # Test with 'by' parameter and custom title
        plot_punchcard(punchcard_data_with_by, by='by', title='Custom Title')
    
    def test_plot_punchcard_no_matplotlib(self, punchcard_data, monkeypatch):
        """Test punchcard plotting when matplotlib is not available."""
        # Mock matplotlib import to simulate it not being available
        monkeypatch.setattr('gitpandas.utilities.plotting.HAS_MPL', False)
        
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
        monkeypatch.setattr('gitpandas.utilities.plotting.HAS_MPL', False)
        
        # Should raise ImportError
        with pytest.raises(ImportError):
            plot_cumulative_blame(cumulative_blame_data) 