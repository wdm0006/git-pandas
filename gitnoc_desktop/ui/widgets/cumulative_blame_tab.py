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
        # Remove previous content (including chart canvas if it exists)
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater() # Properly delete widgets

        # Add placeholder label
        placeholder = QLabel(message)
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.content_layout.addWidget(placeholder)

        # Reset state
        self.refresh_button.setEnabled(False)
        self.refresh_time_label.setText("Last refreshed: N/A")
        self.repo = None
        self.blame_data = pd.DataFrame()
        self.last_refreshed = None

        # Re-add the (now empty) canvas to the layout but keep it hidden
        self.figure.clear() # Clear the figure
        self.ax = self.figure.add_subplot(111) # Re-add subplot axes
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setVisible(False)
        self.content_layout.addWidget(self.canvas)


    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def populate_ui(self, repo, blame_data, refreshed_at):
        """Populates the UI with fetched blame data and refresh time."""
        self._show_placeholder() # Clear previous state/placeholders
        self.repo = repo
        self.last_refreshed = refreshed_at

        if refreshed_at:
            timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error") # Indicate potential issue

        if blame_data is None or blame_data.empty:
            error_message = "Failed to load cumulative blame data."
            if blame_data is not None and blame_data.empty:
                error_message = "No cumulative blame data found for this repository/branch."
            self._show_placeholder(f"<font color='orange'>{error_message}</font>")
            self._hide_loading()
            self.refresh_button.setEnabled(self.repo is not None)
            return

        self.blame_data = blame_data

        # Clear placeholder before drawing
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget and isinstance(widget, QLabel): # Remove only the placeholder label
                 widget.deleteLater()
            elif widget and widget == self.canvas: # Detach canvas temporarily
                self.content_layout.removeWidget(self.canvas)


        # --- Draw Chart ---
        try:
            self.ax.clear() # Clear previous plot
            if not self.blame_data.empty:
                # Ensure index is datetime
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
                self.canvas.draw()
                self.canvas.setVisible(True) # Show the canvas now
            else:
                 # Should have been caught earlier, but handle defensively
                 self._show_placeholder("<font color='orange'>No data to plot.</font>")


        except Exception as e:
            print(f"Error plotting cumulative blame: {e}") # Log error
            traceback.print_exc()
            # Show error state in UI
            self._show_placeholder(f"<font color='red'>Error generating chart: {e}</font>")


        # --- Final Touches ---
        # Re-add canvas if it wasn't already (e.g., if placeholder was shown initially)
        if self.canvas.parent() is None:
            self.content_layout.addWidget(self.canvas)

        self._hide_loading()
        self.refresh_button.setEnabled(self.repo is not None)