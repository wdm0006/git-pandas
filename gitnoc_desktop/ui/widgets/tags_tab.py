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
    QScrollArea,
)
from PySide6.QtCore import Qt

from ..widgets.loading_overlay import LoadingOverlay  # Import LoadingOverlay

class TagsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None
        self.last_refreshed = None
        
        # Create main layout
        self.main_layout = QVBoxLayout(self)
        
        # Create header layout
        header_layout = QHBoxLayout()
        
        # Add refresh button
        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setEnabled(False)
        header_layout.addWidget(self.refresh_button)
        
        # Add refresh time label
        self.refresh_time_label = QLabel("Last refreshed: Never")
        header_layout.addWidget(self.refresh_time_label)
        header_layout.addStretch()
        
        self.main_layout.addLayout(header_layout)
        
        # Create scroll area for content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Create widget for scroll area
        scroll_content = QWidget()
        self.content_layout = QVBoxLayout(scroll_content)
        scroll.setWidget(scroll_content)
        
        self.main_layout.addWidget(scroll)
        
        # Add loading overlay
        self.loading_overlay = LoadingOverlay(self)
        self.loading_overlay.hide()
        
        # Show initial placeholder
        self._show_placeholder()

    def _show_placeholder(self):
        """Shows placeholder when no repository is loaded."""
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        
        placeholder = QLabel("No repository loaded")
        placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.content_layout.addWidget(placeholder)

    def _add_table(self, df, columns, layout):
        """Adds a table widget populated with DataFrame data."""
        table = QTableWidget()
        table.setColumnCount(len(columns))
        table.setRowCount(len(df))
        
        # Set headers
        headers = []
        for col in columns:
            # Convert column names to display format
            header = col.replace('_', ' ').title()
            headers.append(header)
        table.setHorizontalHeaderLabels(headers)
        
        # Populate data
        for i, (_, row) in enumerate(df.iterrows()):
            for j, col in enumerate(columns):
                item = QTableWidgetItem(str(row[col]) if pd.notna(row[col]) else '')
                table.setItem(i, j, item)
        
        # Adjust column widths
        table.resizeColumnsToContents()
        table.setMinimumHeight(min(400, table.verticalHeader().length() + 60))
        
        layout.addWidget(table)

    def populate_ui(self, repo, data, refreshed_at):
        """Populates the UI with fetched tags data and refresh time."""
        self._show_placeholder()
        self.repo = repo
        self.last_refreshed = refreshed_at
        
        # Update refresh time label
        if refreshed_at:
            timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")
        
        if data is None or data.get('tags') is None:
            error_label = QLabel("<font color='orange'>Failed to load tags data in background.</font>")
            error_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.content_layout.addWidget(error_label)
            self.refresh_button.setEnabled(self.repo is not None)
            return
        
        tags_df = data['tags']
        
        # Clear Placeholder
        while self.content_layout.count():
            item = self.content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        
        # Add repository header
        repo_label = QLabel(f"Repository: <b>{self.repo.repo_name}</b>")
        self.content_layout.addWidget(repo_label)
        self.content_layout.addSpacing(10)
        
        # Add tags section
        self._add_section_label("Git Tags", self.content_layout)
        
        if isinstance(tags_df, pd.DataFrame) and not tags_df.empty:
            # Reset index to make tag_date and commit_date regular columns
            tags_df = tags_df.reset_index()
            
            # Format dates
            if 'tag_date' in tags_df.columns:
                tags_df['tag_date'] = pd.to_datetime(tags_df['tag_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
            if 'commit_date' in tags_df.columns:
                tags_df['commit_date'] = pd.to_datetime(tags_df['commit_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Define columns to show
            cols_to_show = ['tag', 'tag_date', 'commit_date', 'annotated', 'annotation']
            # Filter to only columns that exist
            cols_to_show = [col for col in cols_to_show if col in tags_df.columns]
            
            self._add_table(tags_df, cols_to_show, self.content_layout)
        else:
            self.content_layout.addWidget(QLabel("<i>No tags found in this repository.</i>"))
        
        self.content_layout.addStretch()
        self._hide_loading()
        self.refresh_button.setEnabled(True)

    def _add_section_label(self, text, layout):
        """Adds a section header label to the layout."""
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label)

    def _show_loading(self):
        """Shows the loading overlay."""
        self.loading_overlay.show()

    def _hide_loading(self):
        """Hides the loading overlay."""
        self.loading_overlay.hide() 