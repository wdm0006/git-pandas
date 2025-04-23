import sys
import traceback
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import git # For exception handling
import time

from PySide6.QtCore import QObject, Signal, QRunnable, Slot, QThreadPool

from gitpandas import Repository

# Need to import the helper from utils
from .utils import get_language_from_extension

logger = logging.getLogger(__name__)

# --- Data Fetching Functions (for Worker threads) --- #
def load_repository_instance(repo_info, cache_backend):
    """Worker function to instantiate Repository.
    
    Args:
        repo_info (dict): Dictionary containing {'path': str, 'default_branch': str|None}.
        cache_backend: The cache backend instance (e.g., DiskCache).

    Returns:
        Repository instance or None if instantiation fails (e.g., default branch issue).
    """
    repo_path = repo_info['path']
    explicit_branch = repo_info.get('default_branch')

    logger.info(f"Attempting to instantiate Repository for {repo_path} (explicit branch: {explicit_branch})")

    try:
        if explicit_branch:
            # Use the explicitly defined branch from config
            repo = Repository(working_dir=repo_path, default_branch=explicit_branch, cache_backend=cache_backend)
        else:
            # No explicit branch saved, let Repository try auto-detection (main/master)
            repo = Repository(working_dir=repo_path, cache_backend=cache_backend)
        
        logger.info(f"Successfully instantiated Repository for {repo_path} using branch: {repo.default_branch}")
        return repo
    except ValueError as e:
        if "Could not detect default branch" in str(e):
            logger.error(f"Failed to instantiate Repository for {repo_path}: {e}. Returning None.")
            return None # Signal failure to MainWindow
        else:
            # Re-raise other ValueErrors
            logger.error(f"Unexpected ValueError instantiating Repository for {repo_path}", exc_info=True)
            raise
    except Exception as e:
        # Catch any other unexpected errors during instantiation
        logger.error(f"Unexpected error instantiating Repository for {repo_path}", exc_info=True)
        raise # Propagate other errors to the worker's error handler

def fetch_overview_data(repo: Repository, force_refresh=False):
    """Worker function to fetch all data needed for the Overview tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching overview data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        blame = repo.blame(committer=False)
        blame.index.name = "author"
        data['blame'] = blame.reset_index().to_dict(orient='records')
        logger.debug(f"Fetched blame data for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching blame for {repo_name}: {e}")
        data['blame'] = None

    try:
        files = repo.list_files()
        lang_counts = {}
        for f in files['file']:
            ext = Path(f).suffix
            if ext:
                lang = get_language_from_extension(ext)
                lang_counts[lang] = lang_counts.get(lang, 0) + 1
        sorted_langs = sorted(lang_counts.items(), key=lambda item: item[1], reverse=True)
        data['lang_counts'] = pd.DataFrame(sorted_langs, columns=['Language', 'Count'])
        logger.debug(f"Fetched language counts for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching language counts for {repo_name}: {e}")
        data['lang_counts'] = None

    try:
        commits = repo.commit_history(limit=5, branch=repo.default_branch)
        if not commits.empty:
             commits['commit_date'] = commits.index.strftime('%Y-%m-%d %H:%M')
        data['commits'] = commits # Pass DataFrame
        logger.debug(f"Fetched commit history for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching commit history for {repo_name}: {e}")
        data['commits'] = None

    try:
        data['bus_factor'] = repo.bus_factor() # Pass DataFrame
        logger.debug(f"Fetched bus factor for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching bus factor for {repo_name}: {e}")
        data['bus_factor'] = None

    try:
        active_branches_list = []
        base_commits = repo.commit_history(limit=1, branch=repo.default_branch)
        if not base_commits.empty:
            cutoff_date = datetime.now(tz=base_commits.index.tz) - timedelta(days=7)
            all_branches = repo.branches()
            logger.debug(f"Checking {len(all_branches)} branches for recent activity in {repo_name}")
            for branch_name in all_branches['branch']:
                try:
                    branch_commits = repo.commit_history(branch=branch_name, limit=1)
                    if not branch_commits.empty and branch_commits.index[0] >= cutoff_date:
                        logger.debug(f"Branch '{branch_name}' is active in {repo_name}.")
                        active_branches_list.append({
                            'branch': branch_name,
                            'last_commit_date': branch_commits.index[0].strftime('%Y-%m-%d %H:%M'),
                            'author': branch_commits.iloc[0]['author']
                        })
                except git.exc.GitCommandError as git_err:
                    logger.warning(f"Could not get history for branch '{branch_name}' in {repo_name}: {git_err}")
                    continue # Ignore branches that fail to load history
                except Exception as branch_err:
                    logger.warning(f"Unexpected error checking branch '{branch_name}' in {repo_name}: {branch_err}")
                    continue
            data['active_branches'] = pd.DataFrame(active_branches_list).sort_values('last_commit_date', ascending=False)
        else:
             logger.warning(f"No base commits found for {repo_name}, cannot determine active branches.")
             data['active_branches'] = pd.DataFrame(columns=['branch', 'last_commit_date', 'author']) # Empty DF
        logger.debug(f"Finished checking active branches for {repo_name}")
    except Exception as e:
        logger.exception(f"Error fetching active branches for {repo_name}: {e}")
        data['active_branches'] = pd.DataFrame(columns=['branch', 'last_commit_date', 'author']) # Empty DF on error

    logger.info(f"Finished fetching overview data for {repo_name}")
    # Return dict with data and timestamp
    return {'data': data, 'refreshed_at': datetime.now()}

def fetch_code_health_data(repo: Repository, force_refresh=False):
    """Worker function to fetch all data needed for the Code Health tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching code health data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        has_cov = repo.has_coverage()
        logger.debug(f"Coverage available for {repo_name}: {has_cov}")
        coverage_df = repo.coverage() if has_cov else pd.DataFrame(columns=['filename', 'coverage'])
        logger.debug(f"Fetched coverage data for {repo_name}")
        change_rates_df = repo.file_change_rates(days=7, coverage=False, branch=repo.default_branch)
        logger.debug(f"Fetched change rates for {repo_name}")
        
        # Get file details and ensure datetime columns are properly handled
        file_details_df = repo.file_detail()
        if not file_details_df.empty and 'last_edit_date' in file_details_df.columns:
            try:
                # Convert to datetime with UTC timezone
                file_details_df['last_edit_date'] = pd.to_datetime(file_details_df['last_edit_date'], utc=True)
            except Exception as e:
                logger.warning(f"Error converting last_edit_date to datetime: {e}")
                file_details_df['last_edit_date'] = pd.NaT
        logger.debug(f"Fetched file details for {repo_name}")

        coverage_df = coverage_df.rename(columns={'filename': 'file'}).set_index('file')
        change_rates_df = change_rates_df.rename_axis('file')
        file_details_df = file_details_df.rename_axis('file')

        merged = pd.merge(file_details_df, change_rates_df, on='file', how='outer')
        merged_data = pd.merge(merged, coverage_df, on='file', how='outer')
        data['merged_data'] = merged_data # Pass DataFrame
        logger.debug(f"Merged health data for {repo_name}, shape: {merged_data.shape}")

        overall_coverage = "N/A"
        if has_cov and 'lines_covered' in merged_data.columns and 'total_lines' in merged_data.columns:
            total_lines = merged_data['total_lines'].fillna(0).sum()
            if total_lines > 0:
                overall_coverage = f"{merged_data['lines_covered'].fillna(0).sum() / total_lines:.1%}"
            else:
                overall_coverage = "0.0%"
        elif has_cov and not merged_data.empty and 'coverage' in merged_data.columns:
             mean_cov = merged_data['coverage'].mean()
             overall_coverage = f"{mean_cov:.1%}" if pd.notna(mean_cov) else "N/A"
        data['overall_coverage'] = overall_coverage
        logger.debug(f"Calculated overall coverage for {repo_name}: {overall_coverage}")

        avg_edit_rate = "N/A"
        if 'edit_rate' in merged_data.columns:
            median_rate = merged_data['edit_rate'].fillna(0).median()
            avg_edit_rate = f"{median_rate:.2f}"
        data['avg_edit_rate'] = avg_edit_rate
        logger.debug(f"Calculated avg edit rate for {repo_name}: {avg_edit_rate}")

    except Exception as e:
        logger.exception(f"Error fetching code health data for {repo_name}: {e}")
        data['merged_data'] = None
        data['overall_coverage'] = "Error"
        # Ensure avg_edit_rate is also set in error case if it was calculated before error
        data.setdefault('avg_edit_rate', "Error") 

    logger.info(f"Finished fetching code health data for {repo_name}")
    # Return dict with data and timestamp
    return {'data': data, 'refreshed_at': datetime.now()}

def fetch_contributor_data(repo: Repository, force_refresh=False):
    """Worker function to fetch data for Contributor Patterns tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching contributor data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        hours_df = repo.hours_estimate(branch=repo.default_branch)
        data['hours'] = hours_df # Pass DataFrame
        logger.debug(f"Fetched hours estimate for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching hours estimate for {repo_name}: {e}")
        data['hours'] = None

    logger.info(f"Finished fetching contributor data for {repo_name}")
    # Return dict with data and timestamp
    return {'data': data, 'refreshed_at': datetime.now()}

def fetch_tags_data(repo: Repository, force_refresh=False):
    """Worker function to fetch data for Tags tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching tags data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        tags_df = repo.tags()
        data['tags'] = tags_df # Pass DataFrame
        logger.debug(f"Fetched tags data for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching tags data for {repo_name}: {e}")
        data['tags'] = None

    logger.info(f"Finished fetching tags data for {repo_name}")
    # Return dict with data and timestamp
    return {'data': data, 'refreshed_at': datetime.now()}

def fetch_cumulative_blame_data(repo: Repository, force_refresh=False):
    """Worker function to fetch data for Cumulative Blame tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching cumulative blame data for {repo_name} (force_refresh={force_refresh}, num_datapoints=50)")
    data = {}
    try:
        # Use a fixed number of datapoints for performance
        kwargs = {'num_datapoints': 50}
        blame_df = repo.cumulative_blame(**kwargs)
        data['blame'] = blame_df # Pass DataFrame
        logger.debug(f"Fetched cumulative blame data for {repo_name}")
    except Exception as e:
        logger.warning(f"Error fetching cumulative blame for {repo_name}: {e}")
        data['blame'] = None

    logger.info(f"Finished fetching cumulative blame data for {repo_name}")
    # Return dict with data and timestamp
    return {'data': data, 'refreshed_at': datetime.now()}

# --- End Data Fetching Functions --- #

def _load_repo_detail(self, repo_path, force_refresh=False):
    repo = self._get_repo_instance(repo_path, force_refresh)
    if repo is None:
        return None, None, None

    commit_history = self._fetch_with_cache(repo, 'commit_history', force_refresh)
    branch_info = self._fetch_with_cache(repo, 'branches', force_refresh)
    tag_info = self._fetch_with_cache(repo, 'tags', force_refresh)
    return commit_history, branch_info, tag_info

def _fetch_cumulative_blame_data(self, repo: Repository, force_refresh=False):
    """Fetches cumulative blame data, using cache unless forced."""
    if repo is None:
        return None
    # Use a fixed number of datapoints for performance
    kwargs = {'num_datapoints': 50}
    blame_data = self._fetch_with_cache(repo, 'cumulative_blame', force_refresh, **kwargs)
    return blame_data

# --- Public Fetch Methods ---
def fetch_repo_detail_async(self, repo_path, force_refresh=False):
    """Fetches basic repo details (history, branches, tags) asynchronously."""
    worker = RepoDetailWorker(self._load_repo_detail, repo_path, force_refresh)
    worker.signals.result.connect(self.repo_detail_fetched)
    worker.signals.finished.connect(self._worker_complete)
    worker.signals.error.connect(self._worker_error)
    self.threadpool.start(worker)
    self.active_workers += 1
    self.check_global_loading_state()

def fetch_contributors_async(self, repo_path, force_refresh=False):
    """Fetches contributor data (hours, bus factor) asynchronously."""
    worker = ContributorsWorker(self._fetch_contributors_data, repo_path, force_refresh)
    worker.signals.result.connect(self.contributors_data_fetched)
    worker.signals.finished.connect(self._worker_complete)
    worker.signals.error.connect(self._worker_error)
    self.threadpool.start(worker)
    self.active_workers += 1
    self.check_global_loading_state()

def fetch_code_health_async(self, repo_path, force_refresh=False):
    """Fetches code health data (file details, coverage) asynchronously."""
    worker = CodeHealthWorker(self._fetch_code_health_data, repo_path, force_refresh)
    worker.signals.result.connect(self.code_health_data_fetched)
    worker.signals.finished.connect(self._worker_complete)
    worker.signals.error.connect(self._worker_error)
    self.threadpool.start(worker)
    self.active_workers += 1
    self.check_global_loading_state()

def fetch_cumulative_blame_async(self, repo_path, force_refresh=False):
    """Fetches cumulative blame data asynchronously."""
    worker = CumulativeBlameWorker(self._fetch_cumulative_blame_data, repo_path, force_refresh)
    worker.signals.result.connect(self.cumulative_blame_data_fetched)
    worker.signals.finished.connect(self._worker_complete)
    worker.signals.error.connect(self._worker_error)
    self.threadpool.start(worker)
    self.active_workers += 1
    self.check_global_loading_state()

# --- Signals for UI updates (to be connected in MainWindow) ---
repo_detail_fetched = Signal(str, object, object, object, datetime) # repo_path, history, branches, tags, timestamp

# --- Worker Signals --- #
class WorkerSignals(QObject):
    result = Signal(str, object, datetime) # repo_path, data, timestamp
    finished = Signal(str) # worker_id
    error = Signal(str, str) # worker_id, error message

# --- Worker Base Class --- #
class Worker(QRunnable):
    def __init__(self, fn, repo_path, force_refresh):
        super(Worker, self).__init__()
        self.fn = fn
        self.repo_path = repo_path
        self.force_refresh = force_refresh
        self.signals = WorkerSignals()
        self.worker_id = f"{fn.__name__}_{repo_path}_{time.time()}" # Unique ID

    @Slot()
    def run(self):
        try:
            start_time = datetime.now()
            result_data = self.fn(self.repo_path, self.force_refresh)
            self.signals.result.emit(self.repo_path, result_data, start_time)
        except Exception as e:
            traceback.print_exc()
            self.signals.error.emit(self.worker_id, str(e))
        finally:
            self.signals.finished.emit(self.worker_id)

# --- Specific Worker Implementations --- #
class RepoDetailWorker(Worker):
    def run(self):
        try:
            start_time = datetime.now()
            history, branches, tags = self.fn(self.repo_path, self.force_refresh)
            # Emit slightly different signal for repo detail
            self.signals.result.emit(self.repo_path, (history, branches, tags), start_time)
        except Exception as e:
            traceback.print_exc()
            self.signals.error.emit(self.worker_id, str(e))
        finally:
            self.signals.finished.emit(self.worker_id)

class ContributorsWorker(Worker):
    # Uses the base Worker run method
    pass

class CodeHealthWorker(Worker):
    # Uses the base Worker run method
    pass

class CumulativeBlameWorker(Worker):
    # Uses the base Worker run method
    pass

# --- Data Fetcher Class --- #
class DataFetcher(QObject):
    # --- Signals for UI updates (to be connected in MainWindow) ---
    repo_detail_fetched = Signal(str, object, datetime) # repo_path, (history, branches, tags), timestamp
    contributors_data_fetched = Signal(str, object, datetime) # repo_path, data_dict, timestamp
    code_health_data_fetched = Signal(str, object, datetime)  # repo_path, data_dict, timestamp
    tags_data_fetched = Signal(str, object, datetime) # repo_path, tags_df, timestamp
    cumulative_blame_data_fetched = Signal(str, object, datetime) # repo_path, blame_df, timestamp
    global_loading_changed = Signal(bool) # True if loading, False if idle
    repo_instantiation_failed = Signal(str) # repo_path - Signal if repo fails basic load

    def __init__(self):
        super().__init__()
        self.threadpool = QThreadPool()
        print(f"Multithreading with maximum {self.threadpool.maxThreadCount()} threads")
        self.repo_cache = {} # Cache Repository objects
        self.active_workers = 0

    def _get_repo_instance(self, repo_path, force_refresh=False):
        if not force_refresh and repo_path in self.repo_cache:
            print(f"Using cached Repository instance for: {repo_path}")
            return self.repo_cache[repo_path]
        try:
            print(f"Creating new Repository instance for: {repo_path}")
            repo = Repository(working_dir=repo_path, verbose=False) # Add cache_backend later if needed
            self.repo_cache[repo_path] = repo
            return repo
        except Exception as e:
            print(f"Error creating Repository instance for {repo_path}: {e}")
            traceback.print_exc()
            return None

    def _fetch_with_cache(self, repo: Repository, method_name: str, force_refresh: bool, **kwargs):
        """Generic fetch method using gitpandas caching if enabled and not forced."""
        # Note: gitpandas built-in caching via @multicache decorator handles the actual caching.
        # This method just centralizes the calling logic and error handling.
        if repo is None:
            return None
        try:
            method_to_call = getattr(repo, method_name)
            print(f"Calling {method_name} for {repo.repo_name} (Force Refresh: {force_refresh}, Args: {kwargs})...")
            start = time.time()
            # The force_refresh logic needs to be handled by clearing the specific cache entry
            # if gitpandas cache backend is used, or simply re-running the method.
            # For now, we just call the method. Caching is handled by the decorator.
            data = method_to_call(**kwargs)
            end = time.time()
            print(f"Finished {method_name} for {repo.repo_name} in {end - start:.2f} seconds.")
            return data
        except Exception as e:
            print(f"Error fetching {method_name} for {repo.repo_name}: {e}")
            traceback.print_exc()
            return None # Return None or empty structure on error

    # --- Internal Data Aggregation/Loading Functions --- #
    def _load_repo_detail(self, repo_path, force_refresh=False):
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None:
            return None, None, None

        # Fetch data using the generic helper
        commit_history = self._fetch_with_cache(repo, 'commit_history', force_refresh)
        branch_info = self._fetch_with_cache(repo, 'branches', force_refresh)
        tag_info = self._fetch_with_cache(repo, 'tags', force_refresh)

        return commit_history, branch_info, tag_info

    def _fetch_contributors_data(self, repo_path, force_refresh=False):
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None: return None
        hours_df = self._fetch_with_cache(repo, 'hours_estimate', force_refresh, committer=True)
        bus_factor_df = self._fetch_with_cache(repo, 'bus_factor', force_refresh)
        return {'hours': hours_df, 'bus_factor': bus_factor_df}

    def _fetch_code_health_data(self, repo_path, force_refresh=False):
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None: return None
        file_details = self._fetch_with_cache(repo, 'file_detail', force_refresh)
        coverage_df = self._fetch_with_cache(repo, 'coverage', force_refresh)
        change_rates = self._fetch_with_cache(repo, 'file_change_rates', force_refresh)

        # Merge data (handle potentially missing data)
        merged_data = pd.DataFrame()
        if file_details is not None:
            merged_data = file_details
        if change_rates is not None:
            # file_details index is 'file', change_rates index is 'filename'
            change_rates = change_rates.reset_index().rename(columns={'filename': 'file'}).set_index('file')
            merged_data = pd.merge(merged_data, change_rates, left_index=True, right_index=True, how='outer')
        if coverage_df is not None and not coverage_df.empty:
            # coverage index is 'filename'
            coverage_df = coverage_df.reset_index().rename(columns={'filename': 'file'}).set_index('file')
            merged_data = pd.merge(merged_data, coverage_df[['coverage']], left_index=True, right_index=True, how='left')
        else:
            merged_data['coverage'] = pd.NA

        # Calculate overall coverage
        overall_coverage = "N/A"
        if coverage_df is not None and not coverage_df.empty and 'lines_covered' in coverage_df.columns and 'total_lines' in coverage_df.columns:
            total_covered = coverage_df['lines_covered'].sum()
            total_lines = coverage_df['total_lines'].sum()
            if total_lines > 0:
                overall_coverage = f"{(total_covered / total_lines):.1%}"

        return {'merged_data': merged_data, 'overall_coverage': overall_coverage}

    def _fetch_cumulative_blame_data(self, repo_path, force_refresh=False):
        """Fetches cumulative blame data, using cache unless forced."""
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None:
            return None
        # Use a fixed number of datapoints for performance
        kwargs = {'num_datapoints': 50}
        blame_data = self._fetch_with_cache(repo, 'cumulative_blame', force_refresh, **kwargs)
        return blame_data

    # --- Public Fetch Methods ---
    def fetch_repo_detail_async(self, repo_path, force_refresh=False):
        """Fetches basic repo details (history, branches, tags) asynchronously."""
        worker = RepoDetailWorker(self._load_repo_detail, repo_path, force_refresh)
        # Connect result signal specifically for RepoDetailWorker
        worker.signals.result.connect(lambda path, result_tuple, ts: self.repo_detail_fetched.emit(path, result_tuple, ts))
        worker.signals.finished.connect(self._worker_complete)
        worker.signals.error.connect(self._worker_error)
        self.threadpool.start(worker)
        self.active_workers += 1
        self.check_global_loading_state()

    def fetch_contributors_async(self, repo_path, force_refresh=False):
        """Fetches contributor data (hours, bus factor) asynchronously."""
        worker = ContributorsWorker(self._fetch_contributors_data, repo_path, force_refresh)
        worker.signals.result.connect(self.contributors_data_fetched)
        worker.signals.finished.connect(self._worker_complete)
        worker.signals.error.connect(self._worker_error)
        self.threadpool.start(worker)
        self.active_workers += 1
        self.check_global_loading_state()

    def fetch_code_health_async(self, repo_path, force_refresh=False):
        """Fetches code health data (file details, coverage) asynchronously."""
        worker = CodeHealthWorker(self._fetch_code_health_data, repo_path, force_refresh)
        worker.signals.result.connect(self.code_health_data_fetched)
        worker.signals.finished.connect(self._worker_complete)
        worker.signals.error.connect(self._worker_error)
        self.threadpool.start(worker)
        self.active_workers += 1
        self.check_global_loading_state()

    def fetch_cumulative_blame_async(self, repo_path, force_refresh=False):
        """Fetches cumulative blame data asynchronously."""
        worker = CumulativeBlameWorker(self._fetch_cumulative_blame_data, repo_path, force_refresh)
        worker.signals.result.connect(self.cumulative_blame_data_fetched)
        worker.signals.finished.connect(self._worker_complete)
        worker.signals.error.connect(self._worker_error)
        self.threadpool.start(worker)
        self.active_workers += 1
        self.check_global_loading_state()

    # --- Worker Completion and State Management ---
    @Slot(str)
    def _worker_complete(self, worker_id):
        print(f"Worker {worker_id} finished.")
        self.active_workers -= 1
        self.check_global_loading_state()

    @Slot(str, str)
    def _worker_error(self, worker_id, error_message):
        print(f"Worker {worker_id} error: {error_message}")
        # Optionally emit a signal to show error in UI status bar

    def check_global_loading_state(self):
        is_loading = self.active_workers > 0
        self.global_loading_changed.emit(is_loading)
