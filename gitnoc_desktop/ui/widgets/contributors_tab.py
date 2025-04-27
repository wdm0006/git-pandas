import logging  # Added logging

import pandas as pd
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QLabel,
    QVBoxLayout,
    QWidget,
)

from .base_tab import BaseTabWidget  # Import base class
from .dataframe_table import DataFrameTable

logger = logging.getLogger(__name__)


class ContributorsTab(BaseTabWidget):
    # Signal to request data refresh
    refresh_requested = Signal()

    def __init__(self, parent=None):
        # Call base init
        super().__init__(tab_title="Contributors", parent=parent)
        # Base handles self.repo, self.last_refreshed, layouts, header widgets

        # --- Specific Widgets for ContributorsTab --- #
        self.contributor_data = {}  # Store the raw data dict if needed
        self.hours_table = None  # Reference to the table widget

        # Create the container widget that will hold the tables/charts
        self.data_container_widget = QWidget()
        self.data_container_layout = QVBoxLayout(self.data_container_widget)
        self.data_container_layout.setContentsMargins(0, 0, 0, 0)
        self.data_container_layout.setSpacing(10)  # Increased spacing a bit

        # Add container to the base content layout (will be shown/hidden)
        self.content_layout.addWidget(self.data_container_widget)
        self.data_container_widget.setVisible(False)  # Initially hidden

        # Call _show_placeholder explicitly to set initial state
        self._show_placeholder()

    def _request_refresh(self):
        # Emit the signal for MainWindow to catch
        self.refresh_requested.emit()

    def _clear_data_container(self):
        """Helper to remove widgets from the specific data container layout."""
        while self.data_container_layout.count():
            item = self.data_container_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(None)
                widget.deleteLater()
        self.hours_table = None
        # Reset other specific widgets if added later (e.g., punchcard chart)

    def _show_placeholder(self, message=None):
        """Overrides base to also hide the data container and clear its content."""
        logger.debug(f"Showing placeholder for {self.header_label.text()}")
        self._clear_data_container()
        self.data_container_widget.setVisible(False)
        super()._show_placeholder(message)
        self.contributor_data = {}  # Reset specific data

    def _show_loading(self, message=None):
        """Overrides base to also hide data container and clear its content."""
        logger.debug(f"Showing loading for {self.header_label.text()}")
        self._clear_data_container()
        self.data_container_widget.setVisible(False)
        super()._show_loading(message)

    def _hide_loading(self):  # Mainly for refresh button state now
        self.refresh_button.setEnabled(self.repo is not None)
        self.refresh_button.setText("Refresh")

    def _show_error(self, message):
        """Overrides base to hide the data container."""
        logger.error(f"Showing error for {self.header_label.text()}: {message}")
        self._clear_data_container()
        self.data_container_widget.setVisible(False)
        super()._show_error(message)

    def populate_ui(self, repo, contributor_data_in, refreshed_at):
        """Populates the Contributors tab UI."""
        logger.debug(f"Populating ContributorsTab UI for repo: {repo.repo_name if repo else 'None'}")
        self.repo = repo
        self._update_refresh_time_label(refreshed_at)

        # Store raw data if needed for other processing
        self.contributor_data = contributor_data_in if contributor_data_in is not None else {}

        # Extract data parts
        hours_df = None
        if isinstance(self.contributor_data, dict):
            hours_df = self.contributor_data.get("hours")
            # punchcard_df = self.contributor_data.get('punchcard') # Example for later

        # --- Validate Data --- #
        # Check if we have at least some data to show
        if not isinstance(hours_df, pd.DataFrame):
            # If hours_df is None or not a DataFrame, show an error/message
            # (We might have other data later, like punchcard, so adjust logic if needed)
            error_msg = "Contributor hours data not available or failed to load."
            if contributor_data_in is None:
                error_msg = "Failed to load contributor data."
            self._show_error(error_msg)
            return

        # --- Data is Valid (at least hours_df): Store and Display --- #

        # Clear previous content from the data container
        self._clear_data_container()

        # --- Populate Data Container --- #
        try:
            # Add Repo Name
            repo_label = QLabel(f"Repository: <b>{self.repo.repo_name}</b>")
            self.data_container_layout.addWidget(repo_label)
            # Removed extra spacing here, _add_section_label adds margin-top

            # --- Estimated Hours Section --- #
            self._add_section_label("Estimated Hours by Contributor", self.data_container_layout)
            if not hours_df.empty:
                # Check if index needs resetting (older versions might have author in index)
                hours_df_display = hours_df.reset_index() if hours_df.index.name == "author" else hours_df.copy()

                # Ensure required columns exist
                if "author" not in hours_df_display.columns or "hours" not in hours_df_display.columns:
                    logger.warning("Hours DataFrame missing expected columns ('author', 'hours').")
                    self.data_container_layout.addWidget(
                        QLabel("<i>Could not display hours data (missing columns).</i>")
                    )
                else:
                    # Round hours for display
                    hours_df_display["hours"] = hours_df_display["hours"].round(1)
                    self.hours_table = DataFrameTable()
                    self.hours_table.set_dataframe(
                        hours_df_display, columns=["author", "hours"], show_index=False, stretch_last=False
                    )
                    self.data_container_layout.addWidget(self.hours_table)
            else:
                self.data_container_layout.addWidget(QLabel("<i>No contributor hours data found.</i>"))

            # --- Punchcard Section (Placeholder) --- #
            # self._add_section_label("Commit Punchcard", self.data_container_layout)
            # Add punchcard logic here if/when punchcard_df is available
            # self.data_container_layout.addWidget(QLabel("<i>Punchcard visualization not yet implemented.</i>"))

            # Add stretch at the end
            self.data_container_layout.addStretch()

            # Make the data container visible and hide the main placeholder
            self.placeholder_status_label.setVisible(False)
            self.data_container_widget.setVisible(True)

        except Exception as e:
            logger.error(f"Error populating contributors tab content: {e}", exc_info=True)
            self._show_error(f"Error displaying contributor data: {e}")
            return

        # --- Final Touches --- #
        self._hide_loading()  # Restore refresh button state

    def _add_section_label(self, text, layout):
        label = QLabel(text)
        label.setStyleSheet("font-weight: bold; margin-top: 10px;")  # Added top margin
        layout.addWidget(label)
