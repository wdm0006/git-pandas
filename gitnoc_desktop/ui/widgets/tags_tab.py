import logging

import pandas as pd
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QLabel,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from .base_tab import BaseTabWidget  # Import base class
from .dataframe_table import DataFrameTable

logger = logging.getLogger(__name__)


# Inherit from BaseTabWidget
class TagsTab(BaseTabWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        # Call base init with title
        super().__init__(tab_title="Tags", parent=parent)
        # Base class handles repo, last_refreshed, main_layout, header_layout,
        # content_layout, placeholder_status_label

        # --- Specific Widgets for Tags Tab --- #
        self.tags_data = pd.DataFrame()
        self.tags_table = None

        # Create scroll area for content - This part is specific and correct
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll_content_widget = QWidget()
        self.scroll_content_layout = QVBoxLayout(self.scroll_content_widget)
        self.scroll_content_layout.setContentsMargins(5, 5, 5, 5)
        self.scroll_content_layout.setSpacing(10)
        self.scroll_area.setWidget(self.scroll_content_widget)

        # Add the scroll area to the content_layout created by the base class
        # Need to ensure placeholder is added *after* scroll area if not already handled
        # Check base class: it adds placeholder_status_label to content_layout.
        # So, just add scroll_area here.
        self.content_layout.addWidget(self.scroll_area)
        self.scroll_area.setVisible(False)  # Hide scroll area initially

        # Call _show_placeholder explicitly to set initial state
        self._show_placeholder()

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _clear_scroll_content(self):
        """Helper to remove widgets specifically from the scroll layout."""
        while self.scroll_content_layout.count():
            item = self.scroll_content_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(None)
                widget.deleteLater()
        self.tags_table = None

    def _show_placeholder(self, message=None):
        """Overrides base just to hide the scroll area."""
        self.scroll_area.setVisible(False)
        self._clear_scroll_content()  # Also clear content when showing placeholder
        super()._show_placeholder(message)  # Call base to handle placeholder label etc.
        self.tags_data = pd.DataFrame()  # Reset specific data

    def _show_loading(self, message=None):
        """Overrides base just to hide the scroll area."""
        self.scroll_area.setVisible(False)
        self._clear_scroll_content()  # Also clear content when showing loading
        super()._show_loading(message)  # Call base to handle placeholder label etc.

    def populate_ui(self, repo, data, refreshed_at):
        """Populates the Tags tab UI with fetched data."""
        logger.debug(f"Populating TagsTab UI for repo: {repo.repo_name if repo else 'None'}")
        self.repo = repo
        self._update_refresh_time_label(refreshed_at)  # Use base method

        # Extract DataFrame (assuming data = {'tags': df, ...} structure)
        tags_df = None
        if isinstance(data, dict) and "tags" in data and isinstance(data["tags"], pd.DataFrame):
            tags_df = data["tags"]

        # Validate Data
        if tags_df is None:
            self._show_error("Failed to load or parse tags data.")  # Use base error display
            self.scroll_area.setVisible(False)  # Ensure scroll area is hidden on error
            return
        if tags_df.empty:
            self._show_error("No tags found in this repository.")  # Use base error display
            self.scroll_area.setVisible(False)  # Ensure scroll area is hidden on error
            return

        # Data is valid, store it
        self.tags_data = tags_df

        # Clear previous content from scroll area
        self._clear_scroll_content()

        # Prepare views - show scroll area, hide placeholder
        self.placeholder_status_label.setVisible(False)
        self.scroll_area.setVisible(True)

        # --- Populate Scroll Content --- #
        try:
            # Populate scroll_content_layout
            repo_label = QLabel(f"Repository: <b>{self.repo.repo_name}</b>")
            self.scroll_content_layout.addWidget(repo_label)  # Add to scroll layout
            self.scroll_content_layout.addSpacing(5)
            self._add_section_label("Git Tags", self.scroll_content_layout)

            # Format dates (using tags_df from earlier)
            tags_df_processed = self.tags_data.copy()
            # Consolidate date parsing
            for col in ["tag_date", "commit_date"]:
                if col in tags_df_processed.columns and not pd.api.types.is_datetime64_any_dtype(
                    tags_df_processed[col]
                ):
                    try:
                        tags_df_processed[col] = pd.to_datetime(tags_df_processed[col], errors="coerce")
                    except Exception as e:
                        logger.warning(f"Could not parse {col}: {e}")

            # Define columns to show & filter to only existing ones
            cols_to_show = ["tag", "tag_date", "commit_date", "annotated", "annotation"]
            cols_present = [col for col in cols_to_show if col in tags_df_processed.columns]

            # Create and add the table
            self.tags_table = DataFrameTable()
            # Format the date columns for display *after* setting the dataframe
            # Or ensure DataFrameTable handles datetime formatting
            self.tags_table.set_dataframe(tags_df_processed, columns=cols_present, show_index=False)
            self.scroll_content_layout.addWidget(self.tags_table)

            self.scroll_content_layout.addStretch()

        except Exception as e:
            logger.error(f"Error populating tags tab content: {e}", exc_info=True)
            self._show_error(f"Error displaying tags data: {e}")  # Use base error display
            self.scroll_area.setVisible(False)  # Ensure scroll area hidden on population error
            return

        # Restore button state using base method
        self._hide_loading()

    def _add_section_label(self, text, layout):
        """Adds a section header label to the specified layout."""
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 5px;")
        layout.addWidget(label)

    def _hide_loading(self):
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message="Failed to load tags data."):
        logger.error(f"Showing error for {self.header_label.text()}: {message}")
        self._clear_scroll_content()
        self.scroll_area.setVisible(False)
        # Let base handle the placeholder label text/visibility and button state
        super()._show_error(message)
        self._hide_loading()

    def _update_refresh_time_label(self, refreshed_at):
        if refreshed_at:
            ts = refreshed_at.strftime("%Y-%m-%d %H:%M:%S")
            self.refresh_time_label.setText(f"Last refreshed: {ts}")
        else:
            self.refresh_time_label.setText("Last refreshed: Error")
