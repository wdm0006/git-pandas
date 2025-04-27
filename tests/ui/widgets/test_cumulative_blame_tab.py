import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import QWidget # Removed QApplication import
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg # Import for spec

# Assuming your project structure allows this import
from ui.widgets.cumulative_blame_tab import CumulativeBlameTab
from gitpandas import Repository # Mocking target

# Removed the custom qt_app fixture

@pytest.fixture
def blame_tab(qapp): # Changed qt_app to qapp (provided by pytest-qt)
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

// ... existing code ... 