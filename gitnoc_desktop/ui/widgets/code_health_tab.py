import logging

import pandas as pd
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QVBoxLayout,
    QWidget,
)

from .base_tab import BaseTabWidget
from .dataframe_table import DataFrameTable

logger = logging.getLogger(__name__)


class CodeHealthTab(BaseTabWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(tab_title="Code Health", parent=parent)
        # Base class handles repo, last_refreshed, main_layout, header_layout,
        # content_layout, placeholder_status_label

        # --- Specific Widgets for CodeHealthTab --- #
        self.merged_data = pd.DataFrame()
        self.health_table = None  # Reference to the table widget

        # Data Container - This part is specific and correct
        self.data_container_widget = QWidget()
        self.data_container_layout = QVBoxLayout(self.data_container_widget)
        self.data_container_layout.setContentsMargins(0, 0, 0, 0)
        self.data_container_layout.setSpacing(5)
        self.data_container_widget.setVisible(False)  # Initially hidden
        # Add the specific container to the content_layout created by the base class
        self.content_layout.addWidget(self.data_container_widget)

        # Call _show_placeholder explicitly to set initial state
        self._show_placeholder()

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def populate_ui(self, repo, health_data, refreshed_at):
        logger.debug(f"Populating CodeHealthTab UI for repo: {repo.repo_name if repo else 'None'}")
        self.repo = repo
        self.last_refreshed = refreshed_at
        if refreshed_at:
            ts = refreshed_at.strftime("%Y-%m-%d %H:%M:%S")
            self.refresh_time_label.setText(f"Last refreshed: {ts}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")
        # Clear previous data
        self._clear_data_container()

        if health_data is None or health_data.get("merged_data") is None:
            self._show_error("Failed to load code health data in background.")
            return

        self.merged_data = health_data.get("merged_data", pd.DataFrame()).fillna(
            {"loc": 0, "complexity": 0, "token_count": 0, "change_rate": 0, "coverage": float("nan")}
        )
        overall_coverage = health_data.get("overall_coverage", "N/A")

        # Populate data container
        stats_layout = QHBoxLayout()
        stats_layout.addWidget(QLabel(f"Repository: <b>{self.repo.repo_name}</b>"))
        stats_layout.addStretch()
        stats_layout.addWidget(QLabel(f"Overall Coverage: <b>{overall_coverage}</b>"))
        self.data_container_layout.addLayout(stats_layout)

        self._add_health_table(self.merged_data)

        # Show data
        self.placeholder_status_label.setVisible(False)
        self.data_container_widget.setVisible(True)
        self._hide_loading()

    def _clear_data_container(self):
        """Helper to remove widgets from the specific data container layout."""
        while self.data_container_layout.count():
            item = self.data_container_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(None)
                widget.deleteLater()
        self.health_table = None

    def _add_health_table(self, df):
        """Creates and adds the DataFrameTable for health data."""
        # Assume df is not None and not empty based on caller check
        logger.debug(f"Adding health table. Columns: {df.columns}")
        display_df = df.copy()

        # Ensure 'file' is a column
        if "file" not in display_df.columns and display_df.index.name == "file":
            display_df.reset_index(inplace=True)
        elif "file" not in display_df.columns:
            # Try to find a likely candidate or log a warning
            if "index" in display_df.columns:
                display_df.rename(columns={"index": "file"}, inplace=True)
            elif display_df.index.name is not None:
                display_df.reset_index(inplace=True)
            else:
                logger.warning("Could not identify file column for health table.")
                # Create a placeholder if needed?
                # display_df['file'] = "Unknown"

        # --- Data Cleaning and Formatting --- # Requires specific knowledge of expected columns

        # Example: Calculate edit_rate if relevant columns exist
        if "abs_rate_of_change" in display_df.columns and "net_rate_of_change" in display_df.columns:
            display_df["edit_rate"] = display_df["abs_rate_of_change"] - display_df["net_rate_of_change"]
            if pd.api.types.is_numeric_dtype(display_df["edit_rate"]):
                display_df["edit_rate"] = display_df["edit_rate"].round(2)
        elif "change_rate" in display_df.columns:  # Use if exists
            display_df["edit_rate"] = display_df["change_rate"].round(2)
        else:
            display_df["edit_rate"] = pd.NA  # Indicate missing data

        # Format coverage
        if "coverage" in display_df.columns:
            display_df["coverage_pct"] = display_df["coverage"].apply(
                lambda x: f"{x:.1%}" if pd.notna(x) and isinstance(x, float | int) else "N/A"
            )
        else:
            display_df["coverage_pct"] = "N/A"

        # Ensure required display columns exist, filling with defaults if necessary
        required_cols = {  # Column name: default value
            "file": "Unknown File",
            "loc": 0,
            "edit_rate": "N/A",
            "coverage_pct": "N/A",
        }
        for col, default in required_cols.items():
            if col not in display_df.columns:
                logger.warning(f"Health data missing expected column '{col}'. Filling with default.")
                display_df[col] = default
            elif col != "file":  # Ensure numeric types for non-file columns if possible
                if col == "loc":
                    display_df[col] = pd.to_numeric(display_df[col], errors="coerce").fillna(0).astype(int)
                # Add similar coercions for other expected numeric cols if needed

        # --- Select and Rename Columns for Display --- #
        # Define the final columns and their display headers
        display_columns_map = {
            "file": "File",
            "loc": "Lines of Code",
            "edit_rate": "Edit Rate",
            "coverage_pct": "Coverage",
            # Add complexity, token_count etc. here if available and desired
        }
        final_columns = list(display_columns_map.keys())
        display_df_final = display_df[final_columns].copy()
        display_df_final.rename(columns=display_columns_map, inplace=True)

        # --- Create and Add Table --- #
        self.health_table = DataFrameTable()
        self.health_table.set_dataframe(display_df_final, columns=list(display_columns_map.values()), show_index=False)
        self.data_container_layout.addWidget(self.health_table, 1)  # Allow table to stretch
