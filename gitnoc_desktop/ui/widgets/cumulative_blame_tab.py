import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton
)
from PySide6.QtCore import Qt, Signal

class CumulativeBlameTab(QWidget):
    refresh_requested = Signal()
    """A widget to display cumulative blame data as a stacked area chart."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None
        self.blame_data = pd.DataFrame()
        self.last_refreshed = None

        # --- Layouts ---
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5)

        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5)

        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)

        # --- Header Widgets ---
        self.header_label = QLabel("Cumulative Blame")
        self.header_label.setStyleSheet("font-weight: bold;")
        self.header_layout.addWidget(self.header_label)
        self.header_layout.addStretch()

        self.refresh_time_label = QLabel("Last refreshed: N/A")
        self.refresh_time_label.setStyleSheet("font-style: italic; color: grey;")
        self.header_layout.addWidget(self.refresh_time_label)
        self.header_layout.addSpacing(10)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setToolTip("Reload data for this tab, bypassing cache")
        self.refresh_button.setEnabled(False)
        self.refresh_button.clicked.connect(self._request_refresh)
        self.header_layout.addWidget(self.refresh_button)

        # --- Chart Area ---
        # Use Matplotlib canvas
        self.figure, self.ax = plt.subplots()
        self.canvas = FigureCanvas(self.figure)
        # Initially hide the canvas until data is loaded
        self.canvas.setVisible(False)
        self.content_layout.addWidget(self.canvas)

        # --- Assembly ---
        self.main_layout.addLayout(self.header_layout)
        self.main_layout.addWidget(self.content_widget, 1)

        self._show_placeholder() # Show placeholder initially

    def _request_refresh(self):
        # Emit the signal instead of calling parent directly
        self.refresh_requested.emit()
        # if self.parent() and hasattr(self.parent(), 'refresh_cumulative_blame_data'):
        #     self.parent().refresh_cumulative_blame_data()

    def _show_placeholder(self, message="<i>Select a repository to view cumulative blame data.</i>"):
        # Remove previous content before adding placeholder
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater() # Properly delete widgets

        # Add placeholder label
        placeholder = QLabel(message)
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.content_layout.addWidget(placeholder)

        # Don't reset other state here, keep it focused on showing the placeholder message
        # self.refresh_button.setEnabled(False)
        # self.refresh_time_label.setText("Last refreshed: N/A")
        # self.repo = None
        # self.blame_data = pd.DataFrame()
        # self.last_refreshed = None

        # Return the label in case the caller wants to manipulate it
        return placeholder

    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def populate_ui(self, repo, blame_data, refreshed_at):
        """Populates the UI with fetched blame data and refresh time."""
        try:
            # Update repo and time label regardless of data validity
            self.repo = repo
            self.last_refreshed = refreshed_at
            if refreshed_at:
                timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
                self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
            else:
                self.refresh_time_label.setText("Last refreshed: Error")

            # Extract DataFrame from dict structure if needed
            if isinstance(blame_data, dict) and 'data' in blame_data and 'blame' in blame_data['data']:
                blame_data = blame_data['data']['blame']

            # Check for invalid data (None or not a DataFrame or empty DataFrame)
            if not isinstance(blame_data, pd.DataFrame) or blame_data.empty:
                error_message = "Failed to load cumulative blame data."
                # Add more specific message if it's an empty DataFrame vs None/other type
                if isinstance(blame_data, pd.DataFrame) and blame_data.empty:
                    error_message = "No cumulative blame data found for this repository/branch."
                elif blame_data is not None:
                     error_message = f"Received invalid data type for blame: {type(blame_data).__name__}"
                self._show_placeholder(f"<font color='orange'>{error_message}</font>")
                self._hide_loading()
                self.refresh_button.setEnabled(self.repo is not None)
                return # Stop processing here

            # --- Data is Valid: Proceed with plotting --- #
            self.blame_data = blame_data

            # Safely clear previous content
            try:
                while self.content_layout.count():
                    item = self.content_layout.takeAt(0)
                    widget = item.widget()
                    if widget and widget != self.canvas:  # Don't delete the canvas
                        widget.deleteLater()
            except RuntimeError:
                # If we hit Qt C++ object deletion issues, recreate the canvas
                self.figure, self.ax = plt.subplots()
                self.canvas = FigureCanvas(self.figure)

            try:
                # Only manipulate the canvas if it's still valid
                if self.canvas and not self.canvas.isValid():
                    # Canvas is invalid, recreate it
                    self.figure, self.ax = plt.subplots()
                    self.canvas = FigureCanvas(self.figure)
                
                self.ax.clear() # Clear previous plot

                # Ensure index is datetime (moved check here)
                if not pd.api.types.is_datetime64_any_dtype(self.blame_data.index):
                        self.blame_data.index = pd.to_datetime(self.blame_data.index)

                # Sort columns (committers) for consistent stacking order
                self.blame_data = self.blame_data.sort_index(axis=1)

                self.ax.stackplot(
                    self.blame_data.index,
                    self.blame_data.values.T, # Transpose needed for stackplot
                    labels=self.blame_data.columns
                )
                self.ax.set_title(f"Cumulative Blame Over Time ({self.repo.repo_name})")
                self.ax.set_xlabel("Date")
                self.ax.set_ylabel("Lines of Code")
                self.ax.legend(loc='upper left', fontsize='small')
                self.figure.autofmt_xdate() # Improve date formatting
                self.figure.tight_layout() # Adjust layout

                # Only add the canvas if it's not already in the layout
                if self.canvas.parent() != self.content_widget:
                    self.content_layout.addWidget(self.canvas)
                
                self.canvas.draw()
                self.canvas.setVisible(True)

            except Exception as e:
                import traceback
                print(f"Error plotting cumulative blame: {e}")
                traceback.print_exc()
                self._show_placeholder(f"<font color='red'>Error generating chart: {e}</font>")

            # --- Final Touches --- #
            self._hide_loading()
            self.refresh_button.setEnabled(self.repo is not None)
            
        except Exception as e:
            import traceback
            print(f"Error in populate_ui: {e}")
            traceback.print_exc()
            self._show_placeholder(f"<font color='red'>Unexpected error: {e}</font>")
            self._hide_loading()
            self.refresh_button.setEnabled(self.repo is not None)