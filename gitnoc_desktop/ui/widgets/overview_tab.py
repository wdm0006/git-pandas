import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import traceback
import logging

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QHeaderView,
    QSplitter,
    QPushButton,
    QGridLayout,
    QSizePolicy
)
from PySide6.QtCore import Qt, Signal

from .dataframe_table import DataFrameTable

# --- Cache File Path ---
# Consistent with main.py
CACHE_DIR = Path.home() / ".gitnoc_desktop"
CACHE_FILE = CACHE_DIR / "cache.json.gz"
# -----------------------

logger = logging.getLogger(__name__)

class OverviewTab(QWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None # To store current repo instance

        # Main layout
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5) # Reduced spacing a bit

        # Header layout (for label and refresh button)
        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5) # Add margin below header
        self.header_label = QLabel("Overview")
        self.header_label.setStyleSheet("font-weight: bold;")
        self.header_layout.addWidget(self.header_label)

        # Add refresh time label to header
        self.refresh_time_label = QLabel("Last refreshed: N/A")
        self.refresh_time_label.setStyleSheet("font-style: italic; color: grey;")
        self.header_layout.addWidget(self.refresh_time_label)
        self.header_layout.addStretch()
        self.header_layout.addSpacing(10) # Add spacing between label and button

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setToolTip("Reload data for this tab, bypassing cache")
        self.refresh_button.setEnabled(False) # Disabled until repo selected
        self.refresh_button.clicked.connect(self._request_refresh)
        self.header_layout.addWidget(self.refresh_button)
        self.main_layout.addLayout(self.header_layout)

        # Placeholder for content area (will hold splitter or placeholder label)
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)
        self.main_layout.addWidget(self.content_widget, 1) # Allow content to stretch

        self._show_placeholder() # Show initial placeholder in content area

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _show_placeholder(self):
        # Clear previous content (splitter or label)
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

        placeholder = QLabel("<i>Select a repository to view the overview.</i>")
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.content_layout.addWidget(placeholder)
        self.refresh_button.setEnabled(False) # Disable refresh if no repo selected/shown
        self.refresh_time_label.setText("Last refreshed: N/A") # Reset time label
        self.repo = None # Clear repo reference

    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")
        # Optionally, could dim or overlay the content_widget here

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None) # Enable only if repo is loaded
        self.refresh_button.setText("Refresh")

    def populate_ui(self, repo, overview_data):
        """Populates the UI with fetched data and cache modification time."""
        logger.debug(f"Populating OverviewTab UI. Received overview_data: {overview_data}")
        self._show_placeholder() # Clear previous content/placeholder
        self.repo = repo # Store repo reference

        # --- Update refresh time label based on cache file modification --- #
        cache_timestamp_str = "N/A"
        try:
            if CACHE_FILE.exists():
                mtime = os.path.getmtime(CACHE_FILE)
                cache_dt = datetime.fromtimestamp(mtime)
                cache_timestamp_str = cache_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                 cache_timestamp_str = "Cache file not found"
        except Exception as e:
            # Log the error appropriately if needed
            # print(f"Error getting cache file time: {e}") # Example for debugging
            cache_timestamp_str = "Error reading cache time"

        self.refresh_time_label.setText(f"Cache updated: {cache_timestamp_str}") # <-- Restore changed label text
        # -------------------------------------------------------------------- #

        if overview_data is None:
            error_label = QLabel("<font color='orange'>Failed to load overview data in background.</font>")
            error_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.content_layout.addWidget(error_label)
            self._hide_loading() # Ensure button is re-enabled even on error
            self.refresh_button.setEnabled(self.repo is not None)
            return

        # --- Data Extraction --- #
        blame_df_list = overview_data.get('blame') # Note: Fetched as list of dicts
        lang_counts = overview_data.get('lang_counts')
        commits_df = overview_data.get('commits')
        bus_factor_df = overview_data.get('bus_factor')
        active_branches_df = overview_data.get('active_branches')

        # --- Clear Placeholder and Setup Splitter --- #
        while self.content_layout.count(): # Clear placeholder label
             item = self.content_layout.takeAt(0)
             widget = item.widget()
             if widget: widget.deleteLater()

        h_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.content_layout.addWidget(h_splitter) # Add splitter to content layout

        # Setup Left and Right Panels within the Splitter
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setSpacing(10)
        h_splitter.addWidget(left_widget)

        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setSpacing(10)
        h_splitter.addWidget(right_widget)

        # --- Populate Left Column --- #
        self._add_section_label(f"Repository: {self.repo.repo_name}", left_layout)
        path_label = QLabel(f"<i>{self.repo.git_dir}</i>")
        path_label.setWordWrap(True)
        left_layout.addWidget(path_label)
        left_layout.addSpacing(10)

        self._add_section_label("Lines of Code by Author (Top 10)", left_layout)
        if blame_df_list: # Check if the list is not empty
             blame_df = pd.DataFrame(blame_df_list)
             table = DataFrameTable()
             table.set_dataframe(blame_df, columns=["author", "loc"], show_index=False)
             left_layout.addWidget(table)
        else:
             left_layout.addWidget(QLabel("<font color='orange'>Blame data not available or failed to load.</font>"))
        left_layout.addSpacing(10)

        self._add_section_label("File Counts by Language", left_layout)
        if lang_counts is not None and not lang_counts.empty:
            table = DataFrameTable()
            table.set_dataframe(lang_counts, columns=['Language', 'Count'], show_index=False, stretch_last=False)
            left_layout.addWidget(table)
        else:
            left_layout.addWidget(QLabel("<font color='orange'>Language data not available.</font>"))
        left_layout.addStretch()

        # --- Populate Right Column --- #
        self._add_section_label(f"Recent Commits (Branch: {self.repo.default_branch})", right_layout)
        if commits_df is not None and not commits_df.empty:
            table = DataFrameTable()
            table.set_dataframe(commits_df, columns=['commit_date', 'author', 'message'], show_index=False)
            right_layout.addWidget(table)
        else:
             right_layout.addWidget(QLabel("<font color='orange'>Commit history not available.</font>"))
        right_layout.addSpacing(10)

        self._add_section_label("Bus Factor", right_layout)
        if bus_factor_df is not None and not bus_factor_df.empty:
            factor_value = bus_factor_df.iloc[0]['bus factor']
            right_layout.addWidget(QLabel(f"Overall Repository Bus Factor: <b>{factor_value}</b>"))
            right_layout.addWidget(QLabel("<i>(Lower means higher risk)</i>"))
        else:
            right_layout.addWidget(QLabel("<i>Bus factor data not available.</i>"))
        right_layout.addSpacing(10)

        self._add_section_label("Recently Active Branches (Last 7 Days)", right_layout)
        if active_branches_df is not None and not active_branches_df.empty:
            table = DataFrameTable()
            table.set_dataframe(active_branches_df, columns=['branch', 'last_commit_date', 'author'], show_index=False)
            right_layout.addWidget(table)
        elif active_branches_df is not None: # Empty DataFrame means no active branches
            right_layout.addWidget(QLabel("<i>No branches found with commits in the last 7 days.</i>"))
        else: # None means an error occurred during fetch
            right_layout.addWidget(QLabel("<font color='orange'>Active branch data not available.</font>"))
        right_layout.addStretch()

        # --- Final UI State --- #
        self._hide_loading() # Restore refresh button state

    def _add_section_label(self, text, layout):
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label) 