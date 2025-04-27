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

# Combine QWidget metaclass and ABCMeta
class QtABCMeta(type(QWidget), ABCMeta):
    pass

# Apply the combined metaclass. Remove ABC from direct inheritance.
class BaseTabWidget(QWidget, metaclass=QtABCMeta):
    """
    Abstract base class for common tab functionality.

    Handles header creation (title, refresh time, refresh button),
    basic state management (placeholder, loading), and refresh signaling.
    Subclasses must implement populate_ui and potentially override
    state management methods for custom content display.
    """
    refresh_requested = Signal() # Signal emitted when refresh button is clicked

    def __init__(self, tab_title="Tab Title", parent=None):
        super().__init__(parent)
        self.repo = None # To store current repo instance
        self.last_refreshed = None

        # --- Main Layout ---
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(5)

        # --- Header Layout ---
        self.header_layout = QHBoxLayout()
        self.header_layout.setContentsMargins(0, 0, 0, 5) # Margin below header

        self.header_label = QLabel(tab_title)
        self.header_label.setStyleSheet("font-weight: bold;")
        self.header_layout.addWidget(self.header_label)

        self.refresh_time_label = QLabel("Last refreshed: N/A")
        self.refresh_time_label.setStyleSheet("font-style: italic; color: grey;")
        self.header_layout.addStretch() # Push time label and button to the right
        self.header_layout.addWidget(self.refresh_time_label)
        self.header_layout.addSpacing(10) # Spacing between time label and button

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setToolTip("Reload data for this tab, bypassing cache")
        self.refresh_button.setEnabled(False) # Disabled until repo selected
        self.refresh_button.clicked.connect(self._request_refresh)
        self.header_layout.addWidget(self.refresh_button)

        self.main_layout.addLayout(self.header_layout)

        # --- Content Area ---
        # Subclasses will add their specific widgets to this layout
        self.content_widget = QWidget()
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(0,0,0,0)
        self.main_layout.addWidget(self.content_widget, 1) # Allow content to stretch

        # --- Placeholder/Status Label (Common) ---
        self.placeholder_status_label = QLabel()
        self.placeholder_status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.placeholder_status_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.placeholder_status_label.setVisible(False) # Hidden initially
        self.content_layout.addWidget(self.placeholder_status_label)

    def _request_refresh(self):
        """Emits the refresh signal when the button is clicked."""
        if self.repo: # Only emit if a repo is loaded
             logger.info(f"Refresh requested for tab: {self.header_label.text()}")
             self.refresh_requested.emit()
        else:
             logger.warning("Refresh requested but no repository is loaded.")

    def _update_refresh_time_label(self, refreshed_at=None):
        """Updates the refresh time label."""
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
        """Hides all widgets directly within content_layout, except the placeholder label."""
        # This method should generally NOT delete widgets, only hide them.
        # Subclasses are responsible for clearing the *contents* of their specific containers.
        for i in range(self.content_layout.count()): # Iterate from start
            item = self.content_layout.itemAt(i)
            widget = item.widget()
            # Hide widgets other than the placeholder
            if widget and widget != self.placeholder_status_label:
                widget.setVisible(False)
                # Do NOT deleteLater() here, as these might be persistent containers

    def _show_placeholder(self, message=None):
        """Shows a placeholder message in the content area."""
        logger.debug(f"Showing placeholder for {self.header_label.text()}")
        self.clear_content_layout() # Clear previous dynamic content
        text = message or f"<i>Select a repository to view the {self.header_label.text().lower()} data.</i>"
        self.placeholder_status_label.setText(text)
        self.placeholder_status_label.setStyleSheet("") # Reset style
        self.placeholder_status_label.setVisible(True)
        self.refresh_button.setEnabled(False)
        self._update_refresh_time_label(None) # Reset time label
        self.repo = None # Clear repo reference

    def _show_loading(self, message=None):
        """Shows a loading message and disables the refresh button."""
        logger.debug(f"Showing loading for {self.header_label.text()}")
        self.clear_content_layout() # Clear previous dynamic content
        text = message or f"<i>Loading {self.header_label.text().lower()} data...</i>"
        self.placeholder_status_label.setText(text)
        self.placeholder_status_label.setStyleSheet("color: grey;") # Style for loading
        self.placeholder_status_label.setVisible(True)
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText("Loading...")

    def _hide_loading(self):
        """Hides the loading message and enables the refresh button."""
        logger.debug(f"Hiding loading for {self.header_label.text()}")
        # Only hide the placeholder/status label if we intend to show actual content
        # Subclasses should call placeholder_status_label.setVisible(False)
        # after adding their content widgets.
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message):
        """Shows an error message in the content area."""
        logger.error(f"Showing error for {self.header_label.text()}: {message}")
        self.clear_content_layout() # Clear previous dynamic content
        self.placeholder_status_label.setText(f"<font color='orange'>{message}</font>")
        self.placeholder_status_label.setVisible(True)
        self._hide_loading() # Restore button state

    @abstractmethod
    def populate_ui(self, repo, data, refreshed_at):
        """
        Abstract method to populate the tab's UI with data.

        Subclasses must implement this method to display their specific content.
        They should:
        1. Store the repo reference (`self.repo = repo`).
        2. Call `self._update_refresh_time_label(refreshed_at)`.
        3. Clear previous content if necessary (often handled by _show_loading/_show_placeholder).
        4. Add their specific widgets to `self.content_layout`.
        5. Hide the `self.placeholder_status_label` if content is successfully displayed.
        6. Call `self._hide_loading()` to restore the refresh button state.
        """
        pass

    def set_repo(self, repo):
        """Sets the current repository and enables the refresh button."""
        self.repo = repo
        self.refresh_button.setEnabled(self.repo is not None)
        if not repo:
             self._show_placeholder() # Show placeholder if repo is cleared 