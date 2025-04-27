import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
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
from .base_tab import BaseTabWidget

# --- Cache File Path ---
# Consistent with main.py
CACHE_DIR = Path.home() / ".gitnoc_desktop"
CACHE_FILE = CACHE_DIR / "cache.json.gz"
# -----------------------

logger = logging.getLogger(__name__)

class OverviewTab(BaseTabWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        # Pass the title to the base class constructor
        super().__init__(tab_title="Overview", parent=parent)
        self.repo = None # To store current repo instance

        # --- Specific widgets for Overview Tab --- #
        # We will add these to self.content_layout in populate_ui
        self.h_splitter = None
        self.left_widget = None
        self.right_widget = None

        # Set initial state after all widgets specific to this tab are defined
        self._show_placeholder()

    def _clear_overview_content(self):
        """Removes the splitter widget added by populate_ui."""
        if self.h_splitter:
            # Remove from layout first
            self.content_layout.removeWidget(self.h_splitter)
            # Schedule for deletion
            self.h_splitter.deleteLater()
            self.h_splitter = None
            # Also nullify references to internal widgets if needed
            self.left_widget = None
            self.right_widget = None

    # --- State Handling Overrides (Minimal) --- #

    # Need to override these minimally to clear specific content before calling base
    def _show_placeholder(self, message=None):
        self._clear_overview_content() # Clear specific widgets first
        super()._show_placeholder(message) # Call base to handle placeholder label

    def _show_loading(self, message=None):
        self._clear_overview_content() # Clear specific widgets first
        super()._show_loading(message) # Call base to handle placeholder label

    # _hide_loading is inherited
    # _show_error is inherited (base _show_error calls clear_content_layout, which now only hides)
    # If base clear_content_layout isn't sufficient, we might need to override _show_error too
    # Let's try without overriding _show_error first.

    # --- Data Population --- #
    def populate_ui(self, repo, overview_data, refreshed_at=None):
        """Populates the Overview tab UI with fetched data."""
        logger.debug(f"Populating OverviewTab UI for repo: {repo.repo_name if repo else 'None'}")

        self.repo = repo
        self._update_refresh_time_label(refreshed_at)

        # Validate data
        if overview_data is None:
            self._show_error("Failed to load overview data.") # Call base method
            self._clear_overview_content() # Ensure splitter is removed on error
            return

        # Data is valid - Clear specific content area first
        self._clear_overview_content()

        # --- Data Extraction --- #
        blame_df_list = overview_data.get('blame')
        lang_counts = overview_data.get('lang_counts')
        commits_df = overview_data.get('commits')
        bus_factor_df = overview_data.get('bus_factor')
        active_branches_df = overview_data.get('active_branches')

        # --- Setup UI Elements --- #
        # Hide placeholder, show content area (implicitly by adding splitter)
        self.placeholder_status_label.setVisible(False)

        # Create and add the splitter to the content_layout managed by the base class
        # Ensure it's created fresh each time
        self.h_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.content_layout.addWidget(self.h_splitter)

        # Setup Left and Right Panels within the Splitter
        self.left_widget = QWidget()
        left_layout = QVBoxLayout(self.left_widget)
        left_layout.setSpacing(10)
        self.h_splitter.addWidget(self.left_widget)

        self.right_widget = QWidget()
        right_layout = QVBoxLayout(self.right_widget)
        right_layout.setSpacing(10)
        self.h_splitter.addWidget(self.right_widget)

        # --- Populate Left & Right Columns --- #
        self._add_section_label(f"Repository: {self.repo.repo_name}", left_layout)
        path_label = QLabel(f"<i>{self.repo.git_dir}</i>")
        path_label.setWordWrap(True)
        left_layout.addWidget(path_label)
        left_layout.addSpacing(10)

        self._add_section_label("Lines of Code by Author (Top 10)", left_layout)
        if blame_df_list: # Check if the list is not empty
            try:
                blame_df = pd.DataFrame(blame_df_list)
                if not blame_df.empty:
                    table = DataFrameTable()
                    table.set_dataframe(blame_df, columns=["author", "loc"], show_index=False)
                    left_layout.addWidget(table)
                else:
                    left_layout.addWidget(QLabel("<i>No blame data found.</i>"))
            except Exception as e:
                logger.error(f"Error creating blame DataFrame or Table: {e}")
                left_layout.addWidget(QLabel("<font color='orange'>Error displaying blame data.</font>"))
        else:
            left_layout.addWidget(QLabel("<font color='orange'>Blame data not available or failed to load.</font>"))
        left_layout.addSpacing(10)

        self._add_section_label("File Counts by Language", left_layout)
        if lang_counts is not None and not lang_counts.empty:
            try:
                table = DataFrameTable()
                # Ensure columns exist before setting
                cols_to_show = [col for col in ['Language', 'Count'] if col in lang_counts.columns]
                table.set_dataframe(lang_counts, columns=cols_to_show, show_index=False, stretch_last=False)
                left_layout.addWidget(table)
            except Exception as e:
                logger.error(f"Error creating language count Table: {e}")
                left_layout.addWidget(QLabel("<font color='orange'>Error displaying language data.</font>"))
        elif lang_counts is not None: # It's an empty DataFrame
            left_layout.addWidget(QLabel("<i>No language data found.</i>"))
        else:
            left_layout.addWidget(QLabel("<font color='orange'>Language data not available.</font>"))
        left_layout.addStretch()

        self._add_section_label(f"Recent Commits (Branch: {self.repo.default_branch if self.repo else 'N/A'})", right_layout)
        if commits_df is not None and not commits_df.empty:
            try:
                table = DataFrameTable()
                cols_to_show = [col for col in ['commit_date', 'author', 'message'] if col in commits_df.columns]
                table.set_dataframe(commits_df, columns=cols_to_show, show_index=False)
                right_layout.addWidget(table)
            except Exception as e:
                logger.error(f"Error creating commits Table: {e}")
                right_layout.addWidget(QLabel("<font color='orange'>Error displaying commit data.</font>"))
        elif commits_df is not None: # Empty
            right_layout.addWidget(QLabel("<i>No commit history found for this branch.</i>"))
        else:
            right_layout.addWidget(QLabel("<font color='orange'>Commit history not available.</font>"))
        right_layout.addSpacing(10)

        self._add_section_label("Bus Factor", right_layout)
        if bus_factor_df is not None and not bus_factor_df.empty:
            try:
                factor_value = bus_factor_df.iloc[0]['bus factor']
                right_layout.addWidget(QLabel(f"Overall Repository Bus Factor: <b>{factor_value}</b>"))
                right_layout.addWidget(QLabel("<i>(Lower means higher risk)</i>"))
            except (KeyError, IndexError) as e:
                logger.error(f"Error extracting bus factor value: {e}")
                right_layout.addWidget(QLabel("<font color='orange'>Error displaying bus factor.</font>"))
        elif bus_factor_df is not None: # Empty
            right_layout.addWidget(QLabel("<i>Bus factor data not calculated.</i>"))
        else:
            right_layout.addWidget(QLabel("<i>Bus factor data not available.</i>"))
        right_layout.addSpacing(10)

        self._add_section_label("Recently Active Branches (Last 7 Days)", right_layout)
        if active_branches_df is not None and not active_branches_df.empty:
            try:
                table = DataFrameTable()
                cols_to_show = [col for col in ['branch', 'last_commit_date', 'author'] if col in active_branches_df.columns]
                table.set_dataframe(active_branches_df, columns=cols_to_show, show_index=False)
                right_layout.addWidget(table)
            except Exception as e:
                logger.error(f"Error creating active branches Table: {e}")
                right_layout.addWidget(QLabel("<font color='orange'>Error displaying active branch data.</font>"))
        elif active_branches_df is not None: # Empty DataFrame means no active branches
            right_layout.addWidget(QLabel("<i>No branches found with commits in the last 7 days.</i>"))
        else: # None means an error occurred during fetch
            right_layout.addWidget(QLabel("<font color='orange'>Active branch data not available.</font>"))
        right_layout.addStretch()

        # --- Final UI State --- #
        self._hide_loading() # Restore refresh button state via base method

    def _add_section_label(self, text, layout):
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label) 