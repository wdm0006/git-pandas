import os
from pathlib import Path
import pandas as pd
import traceback
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QHeaderView,
    QSplitter,
    QTreeWidget,
    QTreeWidgetItem,
    QTextBrowser,
    QPushButton,
    QSizePolicy,
)
from PySide6.QtCore import Qt, Signal

from .dataframe_table import DataFrameTable

class CodeHealthTab(QWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None
        self.merged_data = pd.DataFrame()
        self.last_refreshed = None

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5)

        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5)
        self.header_label = QLabel("Code Health")
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
        self.main_layout.addLayout(self.header_layout)

        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)
        self.content_layout.setSpacing(5)
        self.main_layout.addWidget(self.content_widget, 1)

        # Placeholder Label
        self.placeholder_label = QLabel("<i>Select a repository to view code health data.</i>")
        self.placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.placeholder_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.content_layout.addWidget(self.placeholder_label)

        # Status Label (Loading/Error)
        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.status_label.setVisible(False)
        self.content_layout.addWidget(self.status_label)

        # Data Container
        self.data_container_widget = QWidget()
        self.data_container_layout = QVBoxLayout(self.data_container_widget)
        self.data_container_layout.setContentsMargins(0,0,0,0)
        self.data_container_layout.setSpacing(5)
        self.data_container_widget.setVisible(False)
        self.content_layout.addWidget(self.data_container_widget)

        self._show_placeholder()

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _show_placeholder(self):
        # Show placeholder, hide other views
        self.placeholder_label.setVisible(True)
        self.status_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.refresh_button.setEnabled(False)
        self.refresh_time_label.setText("Last refreshed: N/A")
        self.repo = None
        self.merged_data = pd.DataFrame()
        self.last_refreshed = None

    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")
        # Clear views and show loading indicator
        self.placeholder_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.status_label.setText("<i>Loading code health data...</i>")
        self.status_label.setStyleSheet("color: grey;")
        self.status_label.setVisible(True)

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message="Failed to load code health data."):
        # Clear views and show error message
        self.placeholder_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.status_label.setText(f"<font color='orange'>{message}</font>")
        self.status_label.setVisible(True)
        self._hide_loading()

    def populate_ui(self, repo, health_data, refreshed_at):
        self.repo = repo
        self.last_refreshed = refreshed_at
        if refreshed_at:
            ts = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {ts}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")
        # Hide placeholder and status for actual data
        self.placeholder_label.setVisible(False)
        self.status_label.setVisible(False)
        # Clear previous data
        while self.data_container_layout.count():
            item = self.data_container_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

        if health_data is None or health_data.get('merged_data') is None:
            self._show_error("Failed to load code health data in background.")
            return

        self.merged_data = health_data.get('merged_data', pd.DataFrame()).fillna({
            'loc': 0, 'complexity': 0, 'token_count': 0, 'change_rate': 0, 'coverage': float('nan')
        })
        overall_coverage = health_data.get('overall_coverage', "N/A")

        # Populate data container
        stats_layout = QHBoxLayout()
        stats_layout.addWidget(QLabel(f"Repository: <b>{self.repo.repo_name}</b>"))
        stats_layout.addStretch()
        stats_layout.addWidget(QLabel(f"Overall Coverage: <b>{overall_coverage}</b>"))
        self.data_container_layout.addLayout(stats_layout)

        self._add_health_table(self.merged_data)

        # Show data
        self.data_container_widget.setVisible(True)

        self._hide_loading()

    def _add_health_table(self, df):
        if df is None or df.empty:
            self.content_layout.addWidget(QLabel("<i>No detailed file health data available.</i>"))
            return

        # Format coverage as percentage and round numeric columns
        df = df.copy()

        # Ensure 'file' is a column, not just the index
        if 'file' not in df.columns:
            df.reset_index(inplace=True)
            # Rename index column if it wasn't named 'file' originally
            if 'index' in df.columns and 'file' not in df.columns:
                 df.rename(columns={'index': 'file'}, inplace=True)
            elif 'level_0' in df.columns and 'file' not in df.columns: # Handle potential multi-index reset
                df.rename(columns={'level_0': 'file'}, inplace=True)

        # Now 'file' should exist as a column
        df['coverage'] = df['coverage'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
        # df['change_rate'] = df['change_rate'].round(2) # This column doesn't exist, use edit_rate later
        # df['complexity'] = df['complexity'].astype(int) # This column doesn't exist
        df['loc'] = df['loc'].astype(int)
        # df['token_count'] = df['token_count'].astype(int) # This column doesn't exist

        # Calculate change rate if columns exist
        if 'abs_rate_of_change' in df.columns and 'net_rate_of_change' in df.columns:
            df['edit_rate'] = df['abs_rate_of_change'] - df['net_rate_of_change']
        else:
            df['edit_rate'] = 0.0 # Default if rates aren't available

        # Add columns if they don't exist (e.g., from older cache)
        if 'coverage' not in df.columns:
            df['coverage'] = pd.NA
        if 'edit_rate' not in df.columns:
            df['edit_rate'] = 0.0

        # Round for display
        if pd.api.types.is_numeric_dtype(df['edit_rate']):
            df['edit_rate'] = df['edit_rate'].round(2)
        if pd.api.types.is_numeric_dtype(df['coverage']):
            df['coverage'] = (df['coverage'] * 100).round(1) # Display as percentage

        # Select and rename columns for display
        # display_df = df[['file', 'loc', 'complexity', 'token_count', 'edit_rate', 'coverage']].copy()
        display_df = df[['file', 'loc', 'edit_rate', 'coverage']].copy()

        # Define columns and their display names
        # columns = ['file', 'loc', 'complexity', 'token_count', 'edit_rate', 'coverage']
        columns = ['file', 'loc', 'edit_rate', 'coverage']

        # Create and configure table
        table = DataFrameTable()
        table.set_dataframe(display_df, columns=columns, show_index=False)
        self.content_layout.addWidget(table, 1) 