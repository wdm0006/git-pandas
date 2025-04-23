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
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QSplitter,
    QTreeWidget,
    QTreeWidgetItem,
    QTextBrowser,
    QPushButton,
)
from PySide6.QtCore import Qt

class CodeHealthTab(QWidget):
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
        self.main_layout.addWidget(self.content_widget, 1)

        self._show_placeholder()

    def _request_refresh(self):
        if self.parent() and hasattr(self.parent(), 'refresh_code_health_data'):
            self.parent().refresh_code_health_data()

    def _show_placeholder(self):
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        placeholder = QLabel("<i>Select a repository to view code health data.</i>")
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.content_layout.addWidget(placeholder)
        self.refresh_button.setEnabled(False)
        self.refresh_time_label.setText("Last refreshed: N/A")
        self.repo = None
        self.merged_data = pd.DataFrame()
        self.last_refreshed = None

    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def populate_ui(self, repo, health_data, refreshed_at):
        """Populates the UI with fetched health data and refresh time."""
        self._show_placeholder()
        self.repo = repo
        self.last_refreshed = refreshed_at

        if refreshed_at:
            timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")

        if health_data is None or health_data.get('merged_data') is None:
            error_label = QLabel("<font color='orange'>Failed to load code health data in background.</font>")
            error_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.content_layout.addWidget(error_label)
            self._hide_loading()
            self.refresh_button.setEnabled(self.repo is not None)
            return

        self.merged_data = health_data.get('merged_data', pd.DataFrame()).fillna({
            'loc': 0, 'complexity': 0, 'token_count': 0, 'change_rate': 0, 'coverage': float('nan')
        })
        overall_coverage = health_data.get('overall_coverage', "N/A")

        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()

        stats_layout = QHBoxLayout()
        stats_layout.addWidget(QLabel(f"Repository: <b>{self.repo.repo_name}</b>"))
        stats_layout.addStretch()
        stats_layout.addWidget(QLabel(f"Overall Coverage: <b>{overall_coverage}</b>"))
        self.content_layout.addLayout(stats_layout)

        self._add_health_table(self.merged_data)

        self._hide_loading()

    def _add_health_table(self, df):
        if df is None or df.empty:
            self.content_layout.addWidget(QLabel("<i>No detailed file health data available.</i>"))
            return

        table = QTableWidget()
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setSortingEnabled(True)

        df_display = df.reset_index()
        df_display['coverage'] = df_display['coverage'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
        df_display['change_rate'] = df_display['change_rate'].round(2)
        df_display['complexity'] = df_display['complexity'].astype(int)
        df_display['loc'] = df_display['loc'].astype(int)
        df_display['token_count'] = df_display['token_count'].astype(int)

        columns = ['file', 'loc', 'complexity', 'token_count', 'change_rate', 'coverage']
        header_labels = ["File Path", "LoC", "Complexity", "Tokens", "Changes/Day (7d)", "Coverage"]

        table.setColumnCount(len(columns))
        table.setHorizontalHeaderLabels(header_labels)
        table.setRowCount(len(df_display))

        for i, row in enumerate(df_display[columns].itertuples(index=False)):
            for j, value in enumerate(row):
                if isinstance(value, (int, float)) and columns[j] != 'coverage':
                    item = QTableWidgetItem()
                    item.setData(Qt.ItemDataRole.DisplayRole, value)
                    item.setData(Qt.ItemDataRole.EditRole, value)
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                else:
                    item = QTableWidgetItem(str(value))

                table.setItem(i, j, item)

        table.resizeColumnsToContents()
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for j in range(1, len(columns)):
             header.setSectionResizeMode(j, QHeaderView.ResizeMode.ResizeToContents)

        self.content_layout.addWidget(table, 1) 