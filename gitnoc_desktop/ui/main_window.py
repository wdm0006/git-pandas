import sys
import logging # Add logging import
from pathlib import Path
import traceback

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QListWidget,
    QLabel,
    QFileDialog,
    QMessageBox,
    QListWidgetItem,
    QTabWidget,
    QSplitter,
    QInputDialog,
)
from PySide6.QtCore import QThreadPool, Qt, Signal
import git # For exception handling

from gitpandas import Repository # Keep Repository import
# Remove EphemeralCache import if DiskCache is now exclusively used

# Project specific imports
from config.loader import load_repositories, save_repositories
from core.workers import Worker
from core.data_fetcher import (
    load_repository_instance,
    fetch_overview_data,
    fetch_code_health_data,
    fetch_contributor_data,
    fetch_tags_data,
    DataFetcher,
    fetch_cumulative_blame_data,
)
from ui.widgets.overview_tab import OverviewTab
from ui.widgets.code_health_tab import CodeHealthTab
from ui.widgets.contributors_tab import ContributorsTab
from ui.widgets.tags_tab import TagsTab
from ui.widgets.cumulative_blame_tab import CumulativeBlameTab
from ui.styles import STYLESHEET
from core.utils import get_language_from_extension # Moved this import here

logger = logging.getLogger(__name__)

class MainWindow(QMainWindow):
    def __init__(self, cache_backend):
        super().__init__()
        logger.info("Initializing MainWindow...")

        # Apply the stylesheet
        self.setStyleSheet(STYLESHEET)

        self.setWindowTitle("GitNOC Desktop")
        # Remove fixed size since we're going fullscreen
        # self.setGeometry(100, 100, 1200, 800)

        self.repositories = load_repositories()  # Dictionary: {name: path}
        self.current_repo_instance = None # Store the active Repository object
        self.pending_workers = 0 # Count of active data loading workers

        # --- Initialize Cache --- #
        self.cache_backend = cache_backend
        if self.cache_backend:
            cache_type = type(self.cache_backend).__name__
            logger.info(f"Using cache backend: {cache_type}")
        else:
            logger.warning("No cache backend provided to MainWindow.")

        # --- Initialize Thread Pool --- #
        self.threadpool = QThreadPool.globalInstance()
        logger.info(f"Using thread pool with max {self.threadpool.maxThreadCount()} threads.")

        # --- Setup Main UI --- #
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.setCentralWidget(main_splitter)

        # Left panel: Repository list and controls
        left_panel_widget = QWidget()
        self.left_layout = QVBoxLayout(left_panel_widget)
        self.left_layout.setContentsMargins(5, 5, 5, 5)
        self.left_layout.setSpacing(5)
        repo_list_label = QLabel("Repositories:")
        self.left_layout.addWidget(repo_list_label)
        self.repo_list_widget = QListWidget()
        self.repo_list_widget.itemSelectionChanged.connect(self.handle_repo_selection)
        self.left_layout.addWidget(self.repo_list_widget, 1) # Give list stretch factor
        button_layout = QHBoxLayout()
        self.add_button = QPushButton("Add")
        self.add_button.setToolTip("Add a new repository")
        self.add_button.clicked.connect(self.add_repository)
        button_layout.addWidget(self.add_button)
        self.remove_button = QPushButton("Remove")
        self.remove_button.setToolTip("Remove selected repository")
        self.remove_button.clicked.connect(self.remove_repository)
        button_layout.addWidget(self.remove_button)
        self.left_layout.addLayout(button_layout)
        main_splitter.addWidget(left_panel_widget)

        # Right panel: Tabbed UI for repository details
        right_panel_widget = QWidget()
        right_layout = QVBoxLayout(right_panel_widget)
        right_layout.setContentsMargins(5, 5, 5, 5)
        self.tab_widget = QTabWidget()
        right_layout.addWidget(self.tab_widget)
        main_splitter.addWidget(right_panel_widget)

        # Set the initial sizes for the splitter (left panel will be ~1/6 of the window)
        main_splitter.setStretchFactor(0, 1)  # Left panel stretch factor
        main_splitter.setStretchFactor(1, 5)  # Right panel stretch factor (increased from 3 to 5)
        
        # Set a maximum width for the left panel
        left_panel_widget.setMaximumWidth(300)  # Limit maximum width
        left_panel_widget.setMinimumWidth(200)  # Ensure it doesn't get too small

        # --- Create Tab Instances --- #
        # Create tabs once, update them later
        self.overview_tab = OverviewTab(self) # Pass parent
        self.code_health_tab = CodeHealthTab(self)
        self.contributors_tab = ContributorsTab(self)
        self.tags_tab = TagsTab(self)
        self.cumulative_blame_tab = CumulativeBlameTab(self) # Instantiate the new tab

        self.tab_widget.addTab(self.overview_tab, "Overview")
        self.tab_widget.addTab(self.code_health_tab, "Code Health")
        self.tab_widget.addTab(self.contributors_tab, "Contributors")
        self.tab_widget.addTab(self.tags_tab, "Tags")
        self.tab_widget.addTab(self.cumulative_blame_tab, "Cumulative Blame") # Add the new tab

        # --- Data Fetcher ---
        self.data_fetcher = DataFetcher()

        self.data_fetcher.code_health_data_fetched.connect(self.code_health_tab.populate_ui)
        self.data_fetcher.contributors_data_fetched.connect(self.contributors_tab.populate_ui)
        self.data_fetcher.tags_data_fetched.connect(self.tags_tab.populate_ui)
        self.data_fetcher.cumulative_blame_data_fetched.connect(self.cumulative_blame_tab.populate_ui) # Connect signal
        self.data_fetcher.global_loading_changed.connect(self._update_global_loading)
        self.data_fetcher.repo_instantiation_failed.connect(self._handle_repo_instantiation_failure)

        # Connect refresh signals from tabs
        self.overview_tab.refresh_requested.connect(self.refresh_overview_data)
        self.code_health_tab.refresh_requested.connect(self.refresh_code_health_data)
        self.contributors_tab.refresh_requested.connect(self.refresh_contributor_data)
        self.tags_tab.refresh_requested.connect(self.refresh_tags_data)
        self.cumulative_blame_tab.refresh_requested.connect(self.refresh_cumulative_blame_data) # Connect refresh

        # --- Initial State --- #
        self.populate_repo_list()
        self._set_loading_state(False) # Initially not loading
        logger.info("MainWindow initialization complete.")

        # Make window fullscreen by default
        self.showFullScreen()

    def _update_global_loading(self, is_loading):
        """Slot to handle the global loading state change."""
        logger.debug(f"Global loading state changed: {is_loading}")
        self._set_loading_state(is_loading)
        # TODO: Add a more prominent global loading indicator?

    def _set_loading_state(self, loading):
        """Enable/disable UI elements based on loading state."""
        logger.debug(f"Setting loading state: {loading}")
        self.repo_list_widget.setEnabled(not loading)
        self.add_button.setEnabled(not loading)
        self.remove_button.setEnabled(not loading)
        self.tab_widget.setEnabled(not loading)
        # TODO: Add a visual indicator like a spinner (QMovie) later
        if loading:
            self.setWindowTitle("GitNOC Desktop - Loading...")
            self.overview_tab._show_placeholder()
            self.code_health_tab._show_placeholder()
            self.contributors_tab._show_placeholder()
            self.tags_tab._show_placeholder()
            self.cumulative_blame_tab._show_placeholder()
        else:
            self.setWindowTitle("GitNOC Desktop")

    def add_repository(self):
        start_dir = str(Path.home())
        selected_items = self.repo_list_widget.selectedItems()
        if selected_items:
            current_path = selected_items[0].data(Qt.ItemDataRole.UserRole)
            if Path(current_path).exists():
                start_dir = str(Path(current_path).parent)

        dir_path = QFileDialog.getExistingDirectory(
            self,
            "Select Git Repository Directory",
            start_dir,
        )

        if dir_path:
            logger.info(f"Attempting to add repository: {dir_path}")
            validated_repo = None
            default_branch = None
            try:
                # First validation attempt (auto-detect branch)
                Repository(working_dir=dir_path, cache_backend=self.cache_backend)
                logger.info(f"Initial validation successful for {dir_path}")
            except git.exc.InvalidGitRepositoryError:
                 logger.warning(f"Invalid Git repository selected: {dir_path}")
                 QMessageBox.warning(self, "Error", f"Not a valid Git repository: {dir_path}")
                 return
            except ValueError as e:
                # Check if it's the specific default branch error
                if "Could not detect default branch" in str(e):
                    logger.warning(f"Could not auto-detect default branch for {dir_path}. Asking user.")
                    branch_name, ok = QInputDialog.getText(
                        self,
                        "Default Branch Required",
                        "Could not detect 'main' or 'master'.\nEnter the default branch name for this repository:",
                    )
                    if ok and branch_name:
                        default_branch = branch_name.strip()
                        try:
                            # Second validation attempt (with specified branch)
                            Repository(working_dir=dir_path, default_branch=default_branch, cache_backend=self.cache_backend)
                            logger.info(f"Validation successful for {dir_path} with branch '{default_branch}'")
                        except Exception as e_retry:
                            logger.exception(f"Failed to validate repository {dir_path} even with branch '{default_branch}'")
                            QMessageBox.critical(self, "Error", f"Failed to validate repository with branch '{default_branch}':\n{str(e_retry)}")
                            return # Abort adding
                    else:
                        logger.info("User cancelled providing default branch.")
                        return # Abort adding if user cancels or enters empty
                else:
                    # Different ValueError occurred during initial validation
                    logger.exception(f"Unexpected ValueError adding repository: {dir_path}")
                    QMessageBox.critical(self, "Error", f"Failed to validate repository: {str(e)}")
                    return
            except Exception as e:
                 # Catch any other unexpected errors during validation
                 logger.exception(f"Unexpected error adding repository: {dir_path}")
                 QMessageBox.critical(self, "Error", f"Failed to add repository: {str(e)}")
                 return

            # If validation succeeded (either initially or with user input)
            repo_name = Path(dir_path).name
            i = 1
            base_name = repo_name
            while repo_name in self.repositories:
                repo_name = f"{base_name}_{i}"
                i += 1

            # Store in the new format
            self.repositories[repo_name] = {
                'path': dir_path,
                'default_branch': default_branch # Will be None if auto-detected
            }
            save_repositories(self.repositories)
            logger.info(f"Successfully added repository '{repo_name}' at {dir_path} (default_branch: {default_branch})")
            self.populate_repo_list()
            items = self.repo_list_widget.findItems(repo_name, Qt.MatchFlag.MatchExactly)
            if items:
                self.repo_list_widget.setCurrentItem(items[0])
            # No success message box needed here, selection triggers load

    def remove_repository(self):
        selected_items = self.repo_list_widget.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Remove Repository", "Please select a repository to remove.")
            return

        selected_item = selected_items[0]
        repo_name = selected_item.text()
        logger.debug(f"Request to remove repository: {repo_name}")

        reply = QMessageBox.question(
            self,
            "Confirm Removal",
            f"Are you sure you want to remove '{repo_name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if reply == QMessageBox.StandardButton.Yes:
            if repo_name in self.repositories:
                logger.info(f"Removing repository: {repo_name}")
                del self.repositories[repo_name]
                save_repositories(self.repositories)
                self.populate_repo_list()
                self.current_repo_instance = None
                self.overview_tab._show_placeholder()
                self.code_health_tab._show_placeholder()
                self.contributors_tab._show_placeholder()
                self.tags_tab._show_placeholder()
                self.cumulative_blame_tab._show_placeholder()
                self._set_loading_state(False)
            else:
                logger.warning(f"Attempted to remove non-existent repository entry: {repo_name}")
                QMessageBox.warning(self, "Error", "Repository not found in internal list.")

    def populate_repo_list(self):
        logger.debug("Populating repository list widget.")
        self.repo_list_widget.clear()
        sorted_repo_names = sorted(self.repositories.keys())
        for name in sorted_repo_names:
            repo_info = self.repositories[name]
            item = QListWidgetItem(name)
            item.setData(Qt.ItemDataRole.UserRole, name)
            tooltip = f"Path: {repo_info['path']}"
            if repo_info.get('default_branch'):
                tooltip += f"\nDefault Branch: {repo_info['default_branch']}"
            item.setToolTip(tooltip)
            self.repo_list_widget.addItem(item)

    def handle_repo_selection(self):
        selected_items = self.repo_list_widget.selectedItems()
        if not selected_items:
            logger.debug("Repository selection cleared.")
            self.current_repo_instance = None
            self.overview_tab._show_placeholder()
            self.code_health_tab._show_placeholder()
            self.contributors_tab._show_placeholder()
            self.tags_tab._show_placeholder()
            self.cumulative_blame_tab._show_placeholder()
            return

        selected_item = selected_items[0]
        repo_name = selected_item.data(Qt.ItemDataRole.UserRole)

        if repo_name not in self.repositories:
            logger.error(f"Selected repository name '{repo_name}' not found in internal list.")
            QMessageBox.warning(self, "Error", f"Repository '{repo_name}' configuration not found.")
            return

        repo_info = self.repositories[repo_name]
        repo_path = repo_info['path']
        logger.info(f"Repository selected: '{repo_name}' at {repo_path} (branch: {repo_info.get('default_branch')})")

        if not repo_path or not Path(repo_path).exists():
             logger.error(f"Selected repository path not found: {repo_path}")
             QMessageBox.warning(self, "Error", f"Path not found for {repo_name}: {repo_path}")
             self.overview_tab._show_placeholder()
             self.code_health_tab._show_placeholder()
             self.contributors_tab._show_placeholder()
             self.tags_tab._show_placeholder()
             self.cumulative_blame_tab._show_placeholder()
             self.current_repo_instance = None
             return

        self._set_loading_state(True)
        self.current_repo_instance = None
        logger.info("Starting repository load worker.")
        repo_loader_worker = Worker(load_repository_instance, repo_info, self.cache_backend)
        repo_loader_worker.signals.result.connect(self._on_repository_loaded)
        repo_loader_worker.signals.error.connect(self._on_repository_load_error)
        self.threadpool.start(repo_loader_worker)

    def _on_repository_load_error(self, error_tuple):
        exctype, value, traceback_str = error_tuple
        logger.error(f"Repository load worker failed: {exctype} - {value}\n{traceback_str}")
        QMessageBox.critical(self, "Repository Load Error", f"Failed to load repository:\n{value}")
        self.current_repo_instance = None
        self._set_loading_state(False) # Ensure loading state is reset on error
        # Show placeholders on error
        self.overview_tab._show_placeholder()
        self.code_health_tab._show_placeholder()
        self.contributors_tab._show_placeholder()
        self.tags_tab._show_placeholder()
        self.cumulative_blame_tab._show_placeholder()

    def _on_repository_loaded(self, repo_instance):
        # Check if the worker returned a valid Repository instance
        if not repo_instance:
             logger.error("Repository load worker failed to return a valid instance (likely default branch issue).")
             # Get the currently selected repo name for the message
             selected_items = self.repo_list_widget.selectedItems()
             repo_name = "Unknown" # Fallback name
             if selected_items:
                 repo_name = selected_items[0].data(Qt.ItemDataRole.UserRole)

             QMessageBox.warning(
                 self, 
                 "Repository Load Failed", 
                 f"Could not load repository '{repo_name}'.\n\n"
                 f"Its default branch could not be automatically determined (neither 'main' nor 'master' found).\n\n"
                 f"Please remove the repository from the list and add it again. You will be prompted to enter the correct default branch name."
             )
             self._set_loading_state(False) # Reset loading state
             # Clear selection and show placeholders
             self.repo_list_widget.clearSelection()
             self.current_repo_instance = None
             self.overview_tab._show_placeholder()
             self.code_health_tab._show_placeholder()
             self.contributors_tab._show_placeholder()
             self.tags_tab._show_placeholder()
             self.cumulative_blame_tab._show_placeholder()
             return

        # --- Proceed if repo_instance is valid --- #
        logger.info(f"Repository {repo_instance.repo_name} loaded. Starting initial data fetch workers.")
        self.current_repo_instance = repo_instance
        self.pending_workers = 0 # Reset counter for initial load

        # Start initial data fetch for all tabs (without force_refresh)
        self._start_data_fetch_worker(fetch_overview_data, self._handle_overview_result)
        self._start_data_fetch_worker(fetch_code_health_data, self._handle_code_health_result)
        self._start_data_fetch_worker(fetch_contributor_data, self._handle_contributor_result)
        self._start_data_fetch_worker(fetch_tags_data, self._handle_tags_result)
        self._start_data_fetch_worker(fetch_cumulative_blame_data, self._handle_cumulative_blame_result)

        # Check if any workers were actually started, if not, reset loading state
        if self.pending_workers == 0:
            logger.warning("No data fetch workers started after repository load.")
            self._set_loading_state(False)
        else:
            logger.info(f"Queued {self.pending_workers} initial data workers.")

    def _start_data_fetch_worker(self, fetch_func, result_handler, force_refresh=False):
        """Helper to start a data fetch worker for the initial load."""
        if not self.current_repo_instance:
            logger.warning(f"Cannot start worker for {fetch_func.__name__}, no repository loaded.")
            return

        logger.debug(f"Queueing worker for {fetch_func.__name__} (force_refresh={force_refresh})")
        self.pending_workers += 1
        # Pass force_refresh to worker
        worker = Worker(fetch_func, self.current_repo_instance, force_refresh=force_refresh)
        worker.signals.result.connect(result_handler)
        # Use the main error handler which decrements pending_workers
        worker.signals.error.connect(self._handle_generic_data_error)
        worker.signals.finished.connect(self._on_worker_finished)
        self.threadpool.start(worker)

    def _on_worker_finished(self):
        """Called when ANY initial data worker finishes."""
        self.pending_workers -= 1
        logger.debug(f"Initial data worker finished. Pending: {self.pending_workers}")
        if self.pending_workers <= 0:
            logger.info("All initial data workers finished.")
            self.pending_workers = 0 # Ensure it's not negative
            self._set_loading_state(False) # Reset global loading state

    # --- Result Handlers for Initial Load --- #
    def _handle_overview_result(self, result_dict):
        """Handles the result from the overview data worker."""
        # result_dict contains { 'data': data, 'refreshed_at': datetime }
        data = result_dict.get('data')
        refreshed_at = result_dict.get('refreshed_at')

        # Check if we still have a current repo instance (user might have cleared selection)
        if self.current_repo_instance:
            logger.debug(f"Populating overview tab for {self.current_repo_instance.repo_name} at {refreshed_at}")
            # Pass refreshed_at to update the UI timestamp
            self.overview_tab.populate_ui(self.current_repo_instance, data, refreshed_at)
        else:
            logger.debug("Received overview data but no repository is currently selected. Ignoring.")
        # self._on_worker_finished() # <--- Let finished signal handle this now

    def _handle_code_health_result(self, result_dict):
        logger.debug("Received initial code health data result.")
        if self.current_repo_instance:
            # Pass data and timestamp to populate_ui
            self.code_health_tab.populate_ui(self.current_repo_instance, result_dict['data'], result_dict['refreshed_at'])
        else:
            logger.warning("Received code health data but no current repo instance.")

    def _handle_contributor_result(self, result_dict):
        logger.debug("Received initial contributor data result.")
        if self.current_repo_instance:
             # Pass data and timestamp to populate_ui
             self.contributors_tab.populate_ui(self.current_repo_instance, result_dict['data'], result_dict['refreshed_at'])
        else:
             logger.warning("Received contributor data but no current repo instance.")

    def _handle_tags_result(self, result_dict):
        logger.debug("Received initial tags data result.")
        if self.current_repo_instance:
            # Pass data (DataFrame or None) and timestamp to populate_ui
            self.tags_tab.populate_ui(self.current_repo_instance, result_dict['data'], result_dict['refreshed_at'])
        else:
             logger.warning("Received tags data but no current repo instance.")

    def _handle_cumulative_blame_result(self, result_dict):
        logger.debug("Received initial cumulative blame data result.")
        if self.current_repo_instance:
            # Pass data (DataFrame or None) and timestamp to populate_ui
            self.cumulative_blame_tab.populate_ui(self.current_repo_instance, result_dict['data'], result_dict['refreshed_at'])
        else:
            logger.warning("Received cumulative blame data but no current repo instance.")

    def _handle_generic_data_error(self, error_tuple):
        """Handles errors from INITIAL data fetch workers."""
        exctype, value, traceback_str = error_tuple
        # Log the error, but don't show a pop-up for background errors
        logger.error(f"Background data fetch worker failed: {exctype} - {value}\n{traceback_str}")
        # The specific tab's populate_ui should handle None/error data
        # We still need to potentially decrement the pending worker count
        # self._on_worker_finished() # Let the finished signal handle this

    # --- Refresh Handlers for Tabs --- # >>> NEW METHODS START HERE <<<
    def _start_refresh_worker(self, fetch_func, target_tab):
        """Helper to start a refresh worker for a specific tab."""
        if not self.current_repo_instance:
            logger.warning(f"Cannot start refresh worker for {fetch_func.__name__}, no repository loaded.")
            target_tab._hide_loading() # Ensure button is re-enabled
            return

        logger.info(f"Starting REFRESH worker for {fetch_func.__name__} on tab {type(target_tab).__name__}")
        target_tab._show_loading()

        worker = Worker(fetch_func, self.current_repo_instance, force_refresh=True)

        # Connect result: unpack dict, check instance, call populate_ui
        # Handle OverviewTab refresh by using its result handler
        if target_tab is self.overview_tab:
            # Reuse the existing overview result handler to call populate_ui correctly
            worker.signals.result.connect(self._handle_overview_result)
        else:
            # For other tabs, call populate_ui with timestamp via lambda
            worker.signals.result.connect(
                lambda res_dict: target_tab.populate_ui(
                    self.current_repo_instance, res_dict['data'], res_dict['refreshed_at']
                ) if self.current_repo_instance else None
            )

        # Connect error to a specific handler that knows which tab to update
        worker.signals.error.connect(lambda error_tuple: self._handle_refresh_error(error_tuple, target_tab))

        # Connect finished to the tab's hide_loading method
        # This ensures the loading indicator stops even if populate_ui isn't called (e.g., None result)
        worker.signals.finished.connect(target_tab._hide_loading)

        self.threadpool.start(worker)

    def _handle_refresh_error(self, error_tuple, target_tab):
        """Handles errors specifically from tab refresh workers."""
        exctype, value, traceback_str = error_tuple
        tab_name = type(target_tab).__name__
        logger.error(f"Refresh worker for tab {tab_name} failed: {exctype} - {value}\n{traceback_str}")
        # Optionally show a small error message within the tab?
        # For now, just log it. populate_ui should handle None data if result wasn't emitted.
        # Ensure loading state is reset via the finished signal connection
        # target_tab._hide_loading() # Now handled by finished signal connection

        # Call the tab's specific error handler
        if hasattr(target_tab, '_show_error'):
            target_tab._show_error(f"Error refreshing data: {value}")
        # Note: _hide_loading is called by the finished signal, 
        # and _show_error also calls _hide_loading now to ensure button state is correct.

    def refresh_overview_data(self):
        self._start_refresh_worker(fetch_overview_data, self.overview_tab)

    def refresh_code_health_data(self):
        self._start_refresh_worker(fetch_code_health_data, self.code_health_tab)

    def refresh_contributor_data(self):
        self._start_refresh_worker(fetch_contributor_data, self.contributors_tab)

    def refresh_tags_data(self):
        self._start_refresh_worker(fetch_tags_data, self.tags_tab)

    def refresh_cumulative_blame_data(self):
        self._start_refresh_worker(fetch_cumulative_blame_data, self.cumulative_blame_tab)

    # --- End Refresh Handlers --- #

    def _handle_repo_instantiation_failure(self, repo_path):
        """Handles the signal emitted when Repository instantiation fails."""
        logger.error(f"Received repo_instantiation_failed signal for path: {repo_path}")
        repo_name = "Unknown repo" # Fallback
        for name, info in self.repositories.items():
            if info['path'] == repo_path:
                repo_name = name
                break
        
        QMessageBox.critical(
            self, 
            "Repository Load Error", 
            f"Failed to load repository: {repo_name}\n\n"
            f"Path: {repo_path}\n\n"
            f"This usually means the repository path is invalid or the default branch ('main' or 'master') could not be found.\n"
            f"Please remove and re-add the repository, specifying the default branch if prompted."
        )
        self._set_loading_state(False) # Ensure UI is unlocked
        self._reset_tabs() # Clear tabs

    def closeEvent(self, event):
        # Ensure proper indentation and add a pass statement
        pass # Placeholder to fix indentation error

# Application Execution (if __name__ == "__main__")
# ... (no changes needed here) 