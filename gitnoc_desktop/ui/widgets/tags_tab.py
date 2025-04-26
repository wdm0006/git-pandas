import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QHeaderView,
    QPushButton,
    QScrollArea,
    QSizePolicy,
)
from PySide6.QtCore import Qt, Signal

from .dataframe_table import DataFrameTable

class TagsTab(QWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.repo = None
        self.tags_data = pd.DataFrame() # Store the DataFrame
        self.last_refreshed = None
        
        # Create main layout
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5)
        
        # Create header layout
        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5)
        
        # Add header label
        self.header_label = QLabel("Tags")
        self.header_label.setStyleSheet("font-weight: bold;")
        self.header_layout.addWidget(self.header_label)
        self.header_layout.addStretch()
        
        # Add refresh time label
        self.refresh_time_label = QLabel("Last refreshed: N/A")
        self.refresh_time_label.setStyleSheet("font-style: italic; color: grey;")
        self.header_layout.addWidget(self.refresh_time_label)
        self.header_layout.addSpacing(10)
        
        # Add refresh button
        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setToolTip("Reload data for this tab, bypassing cache")
        self.refresh_button.setEnabled(False)
        self.refresh_button.clicked.connect(self._request_refresh) # Connect to internal slot
        self.header_layout.addWidget(self.refresh_button)
        
        self.main_layout.addLayout(self.header_layout)
        
        # Create scroll area for content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Create widget for scroll area
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        scroll.setWidget(self.content_widget)
        
        self.main_layout.addWidget(scroll)
        
        # Placeholder Label
        self.placeholder_label = QLabel("<i>Select a repository to view tags data.</i>")
        self.placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.placeholder_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.content_layout.addWidget(self.placeholder_label)

        # Status Label (Loading/Error)
        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.status_label.setVisible(False)
        self.content_layout.addWidget(self.status_label)

        # Data Container Widget
        self.data_container_widget = QWidget()
        self.data_container_layout = QVBoxLayout(self.data_container_widget)
        self.data_container_layout.setContentsMargins(0,0,0,0)
        self.data_container_layout.setSpacing(5)
        self.data_container_widget.setVisible(False)
        self.content_layout.addWidget(self.data_container_widget)

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _show_placeholder(self):
        """Shows placeholder when no repository is loaded."""
        self.placeholder_label.setText("<i>No repository loaded</i>")
        self.placeholder_label.setVisible(True)
        self.status_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.refresh_button.setEnabled(False)
        self.refresh_time_label.setText("Last refreshed: N/A")
        self.repo = None
        self.tags_data = pd.DataFrame()
        self.last_refreshed = None

    def _show_loading(self):
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")
        self.placeholder_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.status_label.setText("<i>Loading tags data...</i>")
        self.status_label.setStyleSheet("color: grey;")
        self.status_label.setVisible(True)

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message="Failed to load tags data."):
        self.placeholder_label.setVisible(False)
        self.data_container_widget.setVisible(False)
        self.status_label.setText(f"<font color='orange'>{message}</font>")
        self.status_label.setVisible(True)
        self._hide_loading()

    def populate_ui(self, repo, data, refreshed_at):
        """Populates the UI with fetched tags data and refresh time."""
        self.repo = repo
        self.last_refreshed = refreshed_at
        # Update refresh time label
        if refreshed_at:
            ts = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            self.refresh_time_label.setText(f"Last refreshed: {ts}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")
        # Prepare views
        self.placeholder_label.setVisible(False)
        self.status_label.setVisible(False)
        self.data_container_layout.parent().setVisible(False)
        self.data_container_widget.setVisible(False)
        self.data_container_layout.parent()  # ensure attribute
        # Clear previous data container
        while self.data_container_layout.count():
            item = self.data_container_layout.takeAt(0)
            widget = item.widget()
            if widget: widget.deleteLater()
        
        if data is None or data.get('tags') is None:
            self._show_error("Failed to load tags data in background.")
            return
        
        tags_df = data['tags']
        
        # Populate data container
        repo_label = QLabel(f"Repository: <b>{self.repo.repo_name}</b>")
        self.data_container_layout.addWidget(repo_label)
        self.data_container_layout.addSpacing(10)
        self._add_section_label("Git Tags", self.data_container_layout)
        
        if isinstance(tags_df, pd.DataFrame) and not tags_df.empty:
            # Format dates
            if 'tag_date' in tags_df.index.names:
                tags_df = tags_df.copy()  # Make a copy to avoid modifying the original
                tags_df = tags_df.reset_index()
                tags_df['tag_date'] = pd.to_datetime(tags_df['tag_date'])
            if 'commit_date' in tags_df.index.names or 'commit_date' in tags_df.columns:
                if 'commit_date' in tags_df.index.names:
                    tags_df = tags_df.reset_index()
                tags_df['commit_date'] = pd.to_datetime(tags_df['commit_date'])
            
            # Define columns to show
            cols_to_show = ['tag', 'tag_date', 'commit_date', 'annotated', 'annotation']
            # Filter to only columns that exist
            cols_to_show = [col for col in cols_to_show if col in tags_df.columns]
            
            # Create and add the table
            table = DataFrameTable()
            table.set_dataframe(tags_df, columns=cols_to_show, show_index=False)
            self.data_container_layout.addWidget(table)
        else:
            self.data_container_layout.addWidget(QLabel("<i>No tags found in this repository.</i>"))
        
        self.data_container_layout.addStretch()
        # Show data
        self.data_container_widget.setVisible(True)
        self._hide_loading()
        self.refresh_button.setEnabled(True)

    def _add_section_label(self, text, layout):
        """Adds a section header label to the layout."""
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label) 