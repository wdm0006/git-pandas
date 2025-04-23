import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
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

        # Placeholder/Content Area
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)
        self.main_layout.addWidget(self.content_widget, 1)

        self._show_placeholder()

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _show_placeholder(self):
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()
        placeholder = QLabel("<i>Select a repository to view contributor patterns.</i>")
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.content_layout.addWidget(placeholder)
        self.refresh_button.setEnabled(False)
        self.refresh_time_label.setText("Last refreshed: N/A") # Reset label
        self.repo = None
        self.last_refreshed = None # Reset time

    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def populate_ui(self, repo, contributor_data, refreshed_at):
        """Populates the UI with fetched contributor data and refresh time."""
        self._show_placeholder()
        self.repo = repo
        self.last_refreshed = refreshed_at # Store time

        # Update refresh time label
        if refreshed_at:
            timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")

        if contributor_data is None:
            error_label = QLabel("<font color='orange'>Failed to load contributor data in background.</font>")
            error_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.content_layout.addWidget(error_label)
            self._hide_loading()
            self.refresh_button.setEnabled(self.repo is not None)
            return

        hours_df = contributor_data.get('hours')
        # punchcard_df = contributor_data.get('punchcard') # Add later

        # --- Clear Placeholder --- #
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()

        # --- Populate Content --- #
        repo_label = QLabel(f"Repository: <b>{self.repo.repo_name}</b>")
        self.content_layout.addWidget(repo_label)
        self.content_layout.addSpacing(10)

        self._add_section_label("Estimated Hours by Contributor", self.content_layout)
        if hours_df is not None and not hours_df.empty:
            table = DataFrameTable()
            table.set_dataframe(hours_df, columns=['hours'], show_index=True, stretch_last=False)
            self.content_layout.addWidget(table)
        else:
            self.content_layout.addWidget(QLabel("<i>Hours estimate data not available or failed to load.</i>"))

        # Add punchcard section later
        # self._add_section_label("Commit Punchcard", self.content_layout)
        # if punchcard_df is not None:
        #     pass # Add punchcard visualization
        # else:
        #     self.content_layout.addWidget(QLabel("<i>Punchcard data not available.</i>"))

        self.content_layout.addStretch()
        self._hide_loading()

    def _add_section_label(self, text, layout):
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label) 