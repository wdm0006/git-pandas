import logging
import traceback

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from PySide6.QtCore import Signal
from PySide6.QtWidgets import QVBoxLayout, QWidget

from .base_tab import BaseTabWidget

logger = logging.getLogger(__name__)


class CumulativeBlameTab(BaseTabWidget):
    refresh_requested = Signal()
    DEFAULT_PLACEHOLDER_TEXT = "<i>Select a repository to view the cumulative blame data.</i>"
    """A widget to display cumulative blame data as a stacked area chart."""

    def __init__(self, parent=None):
        super().__init__(tab_title="Cumulative Blame", parent=parent)
        # Base class handles repo, last_refreshed, main_layout, header_layout,
        # content_layout, placeholder_status_label

        # --- Specific Widgets for CumulativeBlameTab --- #
        self.blame_data = pd.DataFrame()

        # Data Container for the chart - This part is specific and correct
        self.data_container_widget = QWidget()
        self.data_container_layout = QVBoxLayout(self.data_container_widget)
        self.data_container_layout.setContentsMargins(0, 0, 0, 0)
        self.data_container_layout.setSpacing(5)

        # Use Matplotlib canvas inside data container
        self.figure, self.ax = plt.subplots()
        self.canvas = FigureCanvas(self.figure)
        self.data_container_layout.addWidget(self.canvas)

        # Add chart container to the main content layout managed by base class
        self.content_layout.addWidget(self.data_container_widget)
        self.data_container_widget.setVisible(False)  # Hide initially

        # Call _show_placeholder explicitly to set initial state
        self._show_placeholder()

    def _request_refresh(self):
        # Emit the signal instead of calling parent directly
        self.refresh_requested.emit()
        # if self.parent() and hasattr(self.parent(), 'refresh_cumulative_blame_data'):
        #     self.parent().refresh_cumulative_blame_data()

    def _clear_data_container(self):
        # Similar to other tabs, clear the layout and reset references
        if self.canvas:
            self.data_container_layout.removeWidget(self.canvas)
            # Maybe just clear plot instead of deleting canvas?
            # self.canvas.figure.clear()
            # Or delete? Deleting might be safer if plot state persists
            self.canvas.deleteLater()
            self.canvas = None
        # Recreate figure/axis/canvas if deleted?
        # Or assume populate_ui will handle it?
        # Let's assume populate_ui handles recreation if needed.
        # For safety, ensure ax is cleared if figure still exists
        if self.figure and self.ax:
            self.ax.clear()
            # self.canvas.draw() # Maybe needed?

        self.blame_data = pd.DataFrame()

    def _show_placeholder(self, message=None):
        self._clear_data_container()  # Clear specific content
        self.data_container_widget.setVisible(False)  # Hide container
        super()._show_placeholder(message)  # Call base for placeholder label
        # Explicitly ensure placeholder is visible
        self.placeholder_status_label.setVisible(True)

    def _show_loading(self, message=None):
        self._clear_data_container()  # Clear specific content
        self.data_container_widget.setVisible(False)  # Hide container
        super()._show_loading(message)  # Call base for placeholder label
        # Explicitly ensure placeholder is visible
        self.placeholder_status_label.setVisible(True)

    def _show_error(self, message):
        """Shows an error message in the content area."""
        # Call parent class method first
        super()._show_error(message)
        # Explicitly ensure placeholder is visible and data container is hidden
        self.placeholder_status_label.setVisible(True)
        self.data_container_widget.setVisible(False)

    def populate_ui(self, repo, blame_data_in, refreshed_at):
        """Populates the UI with fetched blame data and refresh time."""
        logger.debug(f"Populating CumulativeBlameTab UI for repo: {repo.repo_name if repo else 'None'}")
        # [ADD] Log received data structure
        logger.debug(f"Received blame_data_in type: {type(blame_data_in)}")
        if isinstance(blame_data_in, dict):
            logger.debug(f"Received blame_data_in keys: {blame_data_in.keys()}")
            # [ADD] Check for error key first
            if "error" in blame_data_in and blame_data_in["error"] is not None:
                logger.error(f"Error received from fetcher: {blame_data_in['error']}")
                self._show_error(f"Error fetching data: {blame_data_in['error']}")
                return
            # [/ADD] End error check
            if "data" in blame_data_in and isinstance(blame_data_in["data"], dict):
                logger.debug(f"Received blame_data_in['data'] keys: {blame_data_in['data'].keys()}")
                if "cumulative_blame" in blame_data_in["data"]:
                    logger.debug(
                        f"Received blame_data_in['data']['cumulative_blame'] type: {type(blame_data_in['data']['cumulative_blame'])}"
                    )
                    # Log shape if it's a DataFrame
                    if isinstance(blame_data_in["data"]["cumulative_blame"], pd.DataFrame):
                        logger.debug(f"Received DataFrame shape: {blame_data_in['data']['cumulative_blame'].shape}")

        self.repo = repo
        self._update_refresh_time_label(refreshed_at)

        try:
            blame_df = None
            # Extract DataFrame from the nested dict structure returned by the worker
            if isinstance(blame_data_in, dict) and "data" in blame_data_in:
                inner_data = blame_data_in["data"]
                # Check if inner_data is a dict and contains valid DataFrame
                if (isinstance(inner_data, dict) and 
                    "cumulative_blame" in inner_data and 
                    isinstance(inner_data["cumulative_blame"], pd.DataFrame)):
                    blame_df = inner_data["cumulative_blame"]
            # Allow for direct DataFrame input (though worker doesn't do this currently)
            elif isinstance(blame_data_in, pd.DataFrame):
                blame_df = blame_data_in

            # --- Validate Data --- #
            if blame_df is None:
                # This error might now be redundant due to the earlier check, but leave as a fallback
                self._show_error("Failed to load or parse cumulative blame data.")
                return
            if blame_df.empty:
                self._show_error("No cumulative blame data found for this repository/branch.")
                return

            # --- Data is Valid: Store and Plot --- #
            self.blame_data = blame_df

            # Ensure the chart container is visible and placeholder is hidden
            self.placeholder_status_label.setVisible(False)
            self.data_container_widget.setVisible(True)

            # --- Plotting Logic --- #
            try:
                # Recreate canvas if it was deleted by previous error/clear
                if not hasattr(self, "canvas") or self.canvas is None:
                    logger.debug("Recreating Matplotlib canvas")
                    # Ensure figure and ax exist too
                    if not hasattr(self, "figure") or self.figure is None:
                        self.figure, self.ax = plt.subplots()
                    elif not hasattr(self, "ax") or self.ax is None:
                        # If figure exists but ax doesn't (unlikely), clear figure or get ax
                        self.figure.clear()
                        self.ax = self.figure.add_subplot(111)

                    self.canvas = FigureCanvas(self.figure)
                    # Need to re-add canvas to layout if it was removed
                    # Check if layout still exists
                    if hasattr(self, "data_container_layout") and self.data_container_layout:
                        # Remove potential old placeholders if layout was reused
                        while self.data_container_layout.count():
                            item = self.data_container_layout.takeAt(0)
                            widget = item.widget()
                            if widget:
                                widget.deleteLater()
                        # Add the new canvas
                        self.data_container_layout.addWidget(self.canvas)
                    else:
                        logger.error("Cannot re-add canvas, data_container_layout missing!")
                        self._show_error("Internal UI error: Layout missing.")
                        return

                self.ax.clear()  # Clear previous plot

                # Ensure index is datetime
                if not pd.api.types.is_datetime64_any_dtype(self.blame_data.index):
                    try:
                        self.blame_data.index = pd.to_datetime(self.blame_data.index)
                    except Exception as e:
                        logger.error(f"Could not convert index to datetime: {e}")
                        self._show_error("Error processing dates in blame data.")
                        return

                # Sort columns (committers) for consistent stacking order
                self.blame_data = self.blame_data.sort_index(axis=1)

                # Handle case with too many columns for legend
                max_legend_items = 15
                show_legend = len(self.blame_data.columns) <= max_legend_items

                self.ax.stackplot(
                    self.blame_data.index,
                    self.blame_data.values.T,  # Transpose needed
                    labels=self.blame_data.columns,
                )
                repo_name = getattr(self.repo, "repo_name", "Repository")  # Safe access
                self.ax.set_title(f"Cumulative Blame Over Time ({repo_name})")
                self.ax.set_xlabel("Date")
                self.ax.set_ylabel("Lines of Code")
                if show_legend:
                    self.ax.legend(loc="upper left", fontsize="x-small")  # Smaller font
                else:
                    logger.warning("Hiding legend due to too many committers.")
                    # Optionally add a note to the plot?
                    # self.ax.text(0.5, 0.01, 'Legend hidden (too many items)', ...)

                self.figure.autofmt_xdate()  # Improve date formatting
                self.figure.tight_layout()
                self.canvas.draw()  # Redraw the canvas

            except Exception as e:
                logger.error(f"Error plotting cumulative blame: {e}")
                logger.error(traceback.format_exc())
                self._show_error(f"Error generating chart: {e}")
                return  # Error shown, exit

            # --- Final Touches --- #
            # The finished signal from the worker will call the base _hide_loading.

        except Exception as e:
            # Catch-all for unexpected errors during population
            logger.error(f"Unexpected error in populate_ui: {e}")
            logger.error(traceback.format_exc())
            self._show_error(f"An unexpected error occurred: {e}")
