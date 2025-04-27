import logging
from datetime import datetime
from abc import ABC, ABCMeta, abstractmethod

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy
)
from PySide6.QtCore import Qt, Signal

logger = logging.getLogger(__name__)

# Combined metaclass for QWidget and ABC
class QtABCMeta(type(QWidget), ABCMeta):
    pass

class BaseTabWidget(QWidget, metaclass=QtABCMeta):
    """
    Abstract base class for tab widgets with common functionality.
    
    Provides header with title and refresh button, state management,
    and content area for subclasses to populate.
    """
    refresh_requested = Signal()

    def __init__(self, tab_title="Tab Title", parent=None):
        super().__init__(parent)
        self.repo = None
        self.last_refreshed = None

        # Main layout
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5)

        # Header layout
        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5)

        self.header_label = QLabel(tab_title)
        self.header_label.setStyleSheet("font-weight: bold;")
        self.header_layout.addWidget(self.header_label)

        self.refresh_time_label = QLabel("Last refreshed: N/A")
        self.refresh_time_label.setStyleSheet("font-style: italic; color: grey;")
        self.header_layout.addStretch()
        self.header_layout.addWidget(self.refresh_time_label)
        self.header_layout.addSpacing(10)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setToolTip("Reload data for this tab, bypassing cache")
        self.refresh_button.setEnabled(False)
        self.refresh_button.clicked.connect(self._request_refresh)
        self.header_layout.addWidget(self.refresh_button)

        self.main_layout.addLayout(self.header_layout)

        # Content area
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)
        self.main_layout.addWidget(self.content_widget, 1)

        # Placeholder/status label
        self.placeholder_status_label = QLabel()
        self.placeholder_status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.placeholder_status_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.placeholder_status_label.setVisible(False)
        self.content_layout.addWidget(self.placeholder_status_label)

    def _request_refresh(self):
        """Emit refresh signal if a repository is loaded."""
        if self.repo:
             logger.info(f"Refresh requested for tab: {self.header_label.text()}")
             self.refresh_requested.emit()
        else:
             logger.warning("Refresh requested but no repository is loaded.")

    def _update_refresh_time_label(self, refreshed_at=None):
        """Update the last refreshed timestamp display."""
        timestamp_str = "N/A"
        if refreshed_at:
            try:
                timestamp_str = refreshed_at.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logger.warning(f"Error formatting refresh timestamp: {e}")
                timestamp_str = "Error"
        self.refresh_time_label.setText(f"Last refreshed: {timestamp_str}")
        self.last_refreshed = refreshed_at

    def clear_content_layout(self):
        """Hide all widgets in content layout except the placeholder label."""
        for i in range(self.content_layout.count()):
            item = self.content_layout.itemAt(i)
            widget = item.widget()
            if widget and widget != self.placeholder_status_label:
                widget.setVisible(False)

    def _show_placeholder(self, message=None):
        """Display placeholder message when no content is available."""
        logger.debug(f"Showing placeholder for {self.header_label.text()}")
        self.clear_content_layout()
        text = message or f"<i>Select a repository to view the {self.header_label.text().lower()} data.</i>"
        self.placeholder_status_label.setText(text)
        self.placeholder_status_label.setStyleSheet("")
        self.placeholder_status_label.setVisible(True)
        self.refresh_button.setEnabled(False)
        self._update_refresh_time_label(None)
        self.repo = None

    def _show_loading(self, message=None):
        """Display loading message while content is being fetched."""
        logger.debug(f"Showing loading for {self.header_label.text()}")
        self.clear_content_layout()
        text = message or f"<i>Loading {self.header_label.text().lower()} data...</i>"
        self.placeholder_status_label.setText(text)
        self.placeholder_status_label.setStyleSheet("color: grey;")
        self.placeholder_status_label.setVisible(True)
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self):
        """Restore button state after loading is complete."""
        logger.debug(f"Hiding loading for {self.header_label.text()}")
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message):
        """Display error message when content loading fails."""
        logger.error(f"Showing error for {self.header_label.text()}: {message}")
        self.clear_content_layout()
        self.placeholder_status_label.setText(f"<font color='orange'>{message}</font>")
        self.placeholder_status_label.setVisible(True)
        self._hide_loading()

    @abstractmethod
    def populate_ui(self, repo, data, refreshed_at):
        """
        Populate the tab with repository data.
        
        Subclasses must implement this method to display specific content.
        
        Args:
            repo: Repository instance
            data: Data to display
            refreshed_at: Timestamp of data refresh
        """
        pass

    def set_repo(self, repo):
        """Set the current repository and update UI state."""
        self.repo = repo
        self.refresh_button.setEnabled(self.repo is not None)
        if not repo:
             self._show_placeholder() # Show placeholder if repo is cleared 