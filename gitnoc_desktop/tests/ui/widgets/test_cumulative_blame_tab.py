import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import QApplication, QWidget # Import QWidget
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg # Import for spec

# Assuming your project structure allows this import
from ui.widgets.cumulative_blame_tab import CumulativeBlameTab
from gitpandas import Repository # Mocking target


@pytest.fixture
def blame_tab(qtbot):
    """Provides an instance of CumulativeBlameTab for testing."""
    # Mock matplotlib dependencies within the tab's __init__
    # Patch 'FigureCanvas' where it's looked up in the CumulativeBlameTab module
    with patch('matplotlib.pyplot.subplots') as mock_subplots, \
         patch('ui.widgets.cumulative_blame_tab.FigureCanvas') as MockFigureCanvas: # Patch the class lookup

        # Configure mocks
        mock_figure = MagicMock()
        mock_ax = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_ax)

        # Create a real QWidget to be returned by the mocked FigureCanvas constructor
        # This satisfies the addWidget type requirement.
        real_widget_for_canvas = QWidget()
        # Make the *mocked class* return our real QWidget when called
        MockFigureCanvas.return_value = real_widget_for_canvas

        # Create a separate MagicMock to potentially track calls on the canvas instance
        # This won't be the object added to the layout, but can stand in for assertions.
        mock_canvas_methods = MagicMock()
        # If tests need to assert calls on the canvas (like draw), they can use this.
        # We might need to copy attributes/methods from real_widget_for_canvas if needed.


        tab = CumulativeBlameTab()
        qtbot.addWidget(tab) # Add the widget under test to qtbot for cleanup/handling

        # Store mocks/objects on the tab instance for later assertions
        tab._mock_figure = mock_figure
        tab._mock_ax = mock_ax
        # tab.canvas is None after __init__ due to _show_placeholder -> _clear_data_container
        # Provide the real widget instance separately for tests that need it.
        tab._real_widget_for_canvas = real_widget_for_canvas
        # Provide the separate mock for method tracking if needed (though maybe less useful now)
        tab._mock_canvas_instance_methods = mock_canvas_methods
        tab._mock_subplots = mock_subplots
        tab._mock_canvas_class = MockFigureCanvas # The patched class

        yield tab

# --- Test Cases ---

def test_populate_ui_initial_state(blame_tab):
    """Test the initial state of the widget before data is loaded."""
    # Instead of checking visibility, check the text content
    assert blame_tab.DEFAULT_PLACEHOLDER_TEXT in blame_tab.placeholder_status_label.text()


# More tests will be added here for populate_ui logic

@patch('ui.widgets.cumulative_blame_tab.logger') # Mock logger
def test_populate_ui_valid_data(mock_logger, blame_tab):
    """Test populate_ui with a valid, non-empty DataFrame."""
    mock_repo = MagicMock(spec=Repository)
    mock_repo.repo_name = "test-repo"
    
    # Create a sample valid DataFrame
    dates = pd.to_datetime(['2023-01-01', '2023-01-02'])
    data = {'Committer A': [10, 15], 'Committer B': [5, 8]}
    sample_df = pd.DataFrame(data, index=dates)
    
    input_data = {'data': {'cumulative_blame': sample_df}, 'refreshed_at': datetime.now()}

    # Ensure plot functions are mocked on the mock_ax
    blame_tab._mock_ax.stackplot = MagicMock()
    blame_tab._mock_ax.legend = MagicMock()
    # Mock methods on the mock_figure
    blame_tab._mock_figure.autofmt_xdate = MagicMock()
    blame_tab._mock_figure.tight_layout = MagicMock()
    # Mock the draw method on the separate mock canvas instance
    # Note: The actual tab.canvas is a QWidget, so we assert on the separate mock
    blame_tab._mock_canvas_instance_methods.draw = MagicMock()

    # Since populate_ui recreates the canvas (using the mocked class which returns
    # the real widget), we need to add the mock draw method to that specific widget
    # instance *before* calling populate_ui.
    # Retrieve the widget instance stored by the fixture.
    the_widget_canvas = blame_tab._real_widget_for_canvas
    the_widget_canvas.draw = MagicMock()

    # Call the method ONCE
    blame_tab.populate_ui(mock_repo, input_data, input_data['refreshed_at'])

    # --- Assertions ---
    # Instead of checking visibility, verify the plotting calls were made
    blame_tab._mock_ax.clear.assert_called()
    blame_tab._mock_ax.stackplot.assert_called_once()
    # Check if columns were sorted for stackplot labels
    call_args, call_kwargs = blame_tab._mock_ax.stackplot.call_args
    assert list(call_kwargs['labels']) == sorted(sample_df.columns)
    blame_tab._mock_ax.set_title.assert_called_once()
    blame_tab._mock_ax.set_xlabel.assert_called_once()
    blame_tab._mock_ax.set_ylabel.assert_called_once()
    blame_tab._mock_ax.legend.assert_called_once()
    blame_tab._mock_figure.autofmt_xdate.assert_called_once()
    blame_tab._mock_figure.tight_layout.assert_called_once()

    # Assert draw was called on the actual widget canvas instance
    the_widget_canvas.draw.assert_called_once()

    # No error logs expected
    mock_logger.error.assert_not_called()
    mock_logger.warning.assert_not_called()
    # Ensure _show_error was not called (check placeholder text)
    assert "Error fetching data:" not in blame_tab.placeholder_status_label.text()
    assert "Failed to load or parse cumulative blame data." not in blame_tab.placeholder_status_label.text()

@patch('ui.widgets.cumulative_blame_tab.logger')
def test_populate_ui_empty_dataframe(mock_logger, blame_tab):
    """Test populate_ui when the fetched DataFrame is empty."""
    mock_repo = MagicMock(spec=Repository)
    mock_repo.repo_name = "test-repo"
    empty_df = pd.DataFrame(columns=['Committer A'])
    input_data = {'data': {'cumulative_blame': empty_df}, 'refreshed_at': datetime.now()}

    blame_tab.populate_ui(mock_repo, input_data, input_data['refreshed_at'])

    # --- Assertions ---
    # Check for error message in text rather than visibility
    assert "No cumulative blame data found" in blame_tab.placeholder_status_label.text()
    # The error is logged in base_tab, not in the mocked logger, so we don't assert on mock_logger.error

@patch('ui.widgets.cumulative_blame_tab.logger')
def test_populate_ui_error_key(mock_logger, blame_tab):
    """Test populate_ui when the input dict contains an 'error' key."""
    mock_repo = MagicMock(spec=Repository)
    mock_repo.repo_name = "test-repo"
    error_message = "Fetcher crashed"
    input_data = {'error': error_message, 'refreshed_at': datetime.now()}

    blame_tab.populate_ui(mock_repo, input_data, input_data['refreshed_at'])

    # --- Assertions ---
    # Check for error message in text rather than visibility
    assert f"Error fetching data: {error_message}" in blame_tab.placeholder_status_label.text()
    # This specific message is logged in the mocked logger
    mock_logger.error.assert_any_call(f"Error received from fetcher: {error_message}")

@patch('ui.widgets.cumulative_blame_tab.logger')
def test_populate_ui_malformed_data_no_blame_key(mock_logger, blame_tab):
    """Test populate_ui with data missing the 'blame' key."""
    mock_repo = MagicMock(spec=Repository)
    mock_repo.repo_name = "test-repo"
    input_data = {'data': {'wrong_key': pd.DataFrame()}, 'refreshed_at': datetime.now()}

    blame_tab.populate_ui(mock_repo, input_data, input_data['refreshed_at'])

    # --- Assertions ---
    # Check for error message in text rather than visibility
    assert "Failed to load or parse cumulative blame data." in blame_tab.placeholder_status_label.text()
    # The error is logged in base_tab, not in the mocked logger, so we don't assert on mock_logger.error

@patch('ui.widgets.cumulative_blame_tab.logger')
def test_populate_ui_malformed_data_no_data_key(mock_logger, blame_tab):
    """Test populate_ui with data missing the 'data' key."""
    mock_repo = MagicMock(spec=Repository)
    mock_repo.repo_name = "test-repo"
    input_data = {'not_data': {'cumulative_blame': pd.DataFrame()}, 'refreshed_at': datetime.now()}

    blame_tab.populate_ui(mock_repo, input_data, input_data['refreshed_at'])

    # --- Assertions ---
    # Check for error message in text rather than visibility
    assert "Failed to load or parse cumulative blame data." in blame_tab.placeholder_status_label.text()
    # The error is logged in base_tab, not in the mocked logger, so we don't assert on mock_logger.error

@patch('ui.widgets.cumulative_blame_tab.logger')
def test_populate_ui_non_dict_input(mock_logger, blame_tab):
    """Test populate_ui with input that is not a dictionary."""
    mock_repo = MagicMock(spec=Repository)
    mock_repo.repo_name = "test-repo"
    input_data = "I am not a dictionary"
    refreshed_time = datetime.now()

    blame_tab.populate_ui(mock_repo, input_data, refreshed_time)

    # --- Assertions ---
    # Check for error message in text rather than visibility
    assert "Failed to load or parse cumulative blame data." in blame_tab.placeholder_status_label.text()
    # The error is logged in base_tab, not in the mocked logger, so we don't assert on mock_logger.error 