import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QPushButton,
)
from PySide6.QtCore import Qt

class ContributorPatternsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None
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
        if self.parent() and hasattr(self.parent(), 'refresh_contributor_data'):
            self.parent().refresh_contributor_data()

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
            self._add_table(hours_df, ['hours'], self.content_layout, stretch_last=False)
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

    def _add_table(self, df, columns, layout, stretch_last=True):
        table = QTableWidget()
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setSortingEnabled(True)

        if isinstance(df.index, pd.MultiIndex):
            df_display = df.reset_index()
            display_columns = [str(col) for col in df_display.columns if col in columns or col in df.index.names]
            header_labels = [col.replace('_', ' ').title() for col in display_columns]
        elif df.index.name:
            df_display = df.reset_index()
            display_columns = [df.index.name] + columns
            header_labels = [col.replace('_', ' ').title() for col in display_columns]
        else:
            df_display = df
            display_columns = columns
            header_labels = [col.replace('_', ' ').title() for col in display_columns]

        table.setColumnCount(len(display_columns))
        table.setHorizontalHeaderLabels(header_labels)
        table.setRowCount(len(df_display))

        for i, row in enumerate(df_display[display_columns].itertuples(index=False)):
            for j, value in enumerate(row):
                 if isinstance(value, (int, float)):
                     item = QTableWidgetItem()
                     item.setData(Qt.ItemDataRole.DisplayRole, round(value, 1))
                     item.setData(Qt.ItemDataRole.EditRole, value)
                     item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                 else:
                     item = QTableWidgetItem(str(value))
                 table.setItem(i, j, item)

        table.resizeColumnsToContents()
        header = table.horizontalHeader()
        for j in range(len(display_columns)):
            if stretch_last and j == len(display_columns) - 1:
                header.setSectionResizeMode(j, QHeaderView.ResizeMode.Stretch)
            else:
                header.setSectionResizeMode(j, QHeaderView.ResizeMode.ResizeToContents)

        # Limit height
        row_height = table.rowHeight(0) if table.rowCount() > 0 else 25
        header_height = table.horizontalHeader().height()
        max_visible_rows = 15
        table_height = min((table.rowCount() + 1) * row_height + header_height, max_visible_rows * row_height + header_height)
        table.setFixedHeight(table_height)

        layout.addWidget(table) 