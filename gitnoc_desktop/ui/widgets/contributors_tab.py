import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
)
from PySide6.QtCore import Qt, Signal

from .dataframe_table import DataFrameTable

class ContributorPatternsTab(QWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None
        self.contributor_data = {}
        self.last_refreshed = None # Store last refresh time

        # Main layout
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5)

        # Header layout
        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5)
        self.header_label = QLabel("Contributor Patterns")
        self.header_label.setStyleSheet("font-weight: bold;")
        self.header_layout.addWidget(self.header_label)
        self.header_layout.addStretch()

        # Add refresh time label
        self.refresh_time_label = QLabel("Last refreshed: N/A")
        self.refresh_time_label.setStyleSheet("font-style: italic; color: grey;")
        self.header_layout.addWidget(self.refresh_time_label)
        self.header_layout.addSpacing(10)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setToolTip("Reload data for this tab, bypassing cache")
        self.refresh_button.setEnabled(False)
        self.refresh_button.clicked.connect(self._request_refresh)
        self.header_layout.addWidget(self.refresh_button)
        self.main_layout.addLayout(self.header_layout)

        # Content Area - Holds placeholder, loading, error, or data
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)
        self.content_layout.setSpacing(5)
        self.main_layout.addWidget(self.content_widget, 1)

        # Placeholder Label
        self.placeholder_label = QLabel("<i>Select a repository to view contributor patterns.</i>")
        self.placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.placeholder_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.content_layout.addWidget(self.placeholder_label)

        # Status Label (for Loading/Error)
        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.status_label.setVisible(False) # Initially hidden
        self.content_layout.addWidget(self.status_label)

        # Data Container Widget (holds tables, etc.)
        self.data_container_widget = QWidget()
        self.data_container_layout = QVBoxLayout(self.data_container_widget)
        self.data_container_layout.setContentsMargins(0,0,0,0)
        self.data_container_layout.setSpacing(5)
        self.data_container_widget.setVisible(False) # Initially hidden
        self.content_layout.addWidget(self.data_container_widget)

        self._show_placeholder() # Initial state

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _show_placeholder(self):
        self.placeholder_label.setVisible(True)
        self.status_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.refresh_button.setEnabled(False)
        self.refresh_time_label.setText("Last refreshed: N/A") # Reset label
        self.repo = None
        self.last_refreshed = None # Reset time

    def _show_loading(self):
        self.placeholder_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.status_label.setText("<i>Loading contributor data...</i>")
        self.status_label.setStyleSheet("color: grey;") # Reset style
        self.status_label.setVisible(True)
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self): # Mainly for refresh button state now
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message="Failed to load data."):
        """Displays an error message in the content area."""
        self.placeholder_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.status_label.setText(f"<font color='orange'>{message}</font>")
        self.status_label.setVisible(True)
        self._hide_loading() # Re-enable refresh button even on error
        self.refresh_button.setEnabled(self.repo is not None)

    def populate_ui(self, repo, contributor_data, refreshed_at):
        """Populates the UI with fetched contributor data and refresh time."""
        self.repo = repo
        self.last_refreshed = refreshed_at # Store time

        # Update refresh time label
        if refreshed_at:
            timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error") # Or maybe 'N/A'?

        # Clear previous data/error state
        self.placeholder_label.setVisible(False)
        self.status_label.setVisible(False)
        # Clear previous widgets from data container
        while self.data_container_layout.count():
            item = self.data_container_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()

        if contributor_data is None:
            self._show_error("Failed to load contributor data.")
            return

        # --- Data Extraction --- #
        hours_df = contributor_data.get('hours')
        # punchcard_df = contributor_data.get('punchcard') # Add later

        # --- Populate Content --- #
        repo_label = QLabel(f"Repository: <b>{self.repo.repo_name}</b>")
        self.data_container_layout.addWidget(repo_label)
        self.data_container_layout.addSpacing(10)

        self._add_section_label("Estimated Hours by Contributor", self.data_container_layout)
        if hours_df is not None and not hours_df.empty:
            # Reset index to make it a column before displaying
            table = DataFrameTable()
            table.set_dataframe(hours_df, columns=['author', 'hours'], show_index=False, stretch_last=False)
            self.data_container_layout.addWidget(table)
        else:
            self.data_container_layout.addWidget(QLabel("<i>Hours estimate data not available or failed to load.</i>"))

        # Add punchcard section later
        # self._add_section_label("Commit Punchcard", self.data_container_layout)
        # if punchcard_df is not None:
        #     pass # Add punchcard visualization
        # else:
        #     self.data_container_layout.addWidget(QLabel("<i>Punchcard data not available.</i>"))

        self.data_container_layout.addStretch()

        # --- Final UI State --- #
        self.data_container_widget.setVisible(True) # Show the populated container
        self._hide_loading() # Restore refresh button state

    def _add_section_label(self, text, layout):
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label) 