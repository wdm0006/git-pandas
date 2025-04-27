import logging
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import git  # Import git for exception handling
import pandas as pd
from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal, Slot

from gitpandas import Repository

# Need to import the helper from utils
from .utils import get_language_from_extension

logger = logging.getLogger(__name__)


# --- Data Fetching Functions (for Worker threads) --- #
def load_repository_instance(repo_info, cache_backend):
    """
    Instantiate a Repository object for the given repository info.

    Args:
        repo_info (dict): Repository information with {'path': str, 'default_branch': str|None}
        cache_backend: Cache backend instance for the Repository

    Returns:
        Repository: Instantiated Repository object or None if instantiation fails
    """
    repo_path = repo_info["path"]
    explicit_branch = repo_info.get("default_branch")

    logger.info(f"Attempting to instantiate Repository for {repo_path} (explicit branch: {explicit_branch})")

    try:
        if explicit_branch:
            repo = Repository(working_dir=repo_path, default_branch=explicit_branch, cache_backend=cache_backend)
        else:
            repo = Repository(working_dir=repo_path, cache_backend=cache_backend)

        logger.info(f"Successfully instantiated Repository for {repo_path} using branch: {repo.default_branch}")
        return repo
    except ValueError as e:
        if "Could not detect default branch" in str(e):
            logger.error(f"Failed to instantiate Repository for {repo_path}: {e}. Returning None.")
            return None
        else:
            logger.error(f"Unexpected ValueError instantiating Repository for {repo_path}", exc_info=True)
            raise
    except Exception:
        logger.error(f"Unexpected error instantiating Repository for {repo_path}", exc_info=True)
        raise


def fetch_overview_data(repo: Repository, force_refresh=False):
    """
    Fetch data for the Overview tab.

    Args:
        repo: Repository instance
        force_refresh: Whether to bypass cache

    Returns:
        dict: Dictionary containing overview data components
    """
    repo_name = repo.repo_name
    logger.info(f"Fetching overview data for {repo_name} (force_refresh={force_refresh})")

    if force_refresh:
        time.sleep(0.5)  # Add delay on refresh to make loading indicator visible

    data = {}

    # Fetch blame data
    try:
        try:
            blame = repo.blame(committer=False, force_refresh=force_refresh)
            blame.index.name = "author"
            data["blame"] = blame.reset_index().to_dict(orient="records")
            logger.debug(f"Fetched blame data for {repo_name}")
        except KeyError as ke:
            logger.warning(f"KeyError '{ke}' occurred in blame for {repo_name}. Retrying with force_refresh=True.")
            try:
                blame = repo.blame(committer=False, force_refresh=True)
                blame.index.name = "author"
                data["blame"] = blame.reset_index().to_dict(orient="records")
                logger.debug(f"Successfully fetched blame data on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for blame also failed: {retry_e}")
                data["blame"] = None
        except Exception as e:
            logger.warning(f"Error fetching blame for {repo_name}: {e}")
            data["blame"] = None
    except Exception as e:
        logger.warning(f"Error fetching blame for {repo_name}: {e}")
        data["blame"] = None

    # Fetch language statistics
    try:
        try:
            files = repo.list_files(force_refresh=force_refresh)
            lang_counts = {}
            for f in files["file"]:
                ext = Path(f).suffix
                if ext:
                    lang = get_language_from_extension(ext)
                    lang_counts[lang] = lang_counts.get(lang, 0) + 1
            sorted_langs = sorted(lang_counts.items(), key=lambda item: item[1], reverse=True)
            data["lang_counts"] = pd.DataFrame(sorted_langs, columns=["Language", "Count"])
            logger.debug(f"Fetched language counts for {repo_name}")
        except KeyError as ke:
            logger.warning(f"KeyError '{ke}' occurred in list_files for {repo_name}. Retrying with force_refresh=True.")
            try:
                files = repo.list_files(force_refresh=True)
                lang_counts = {}
                for f in files["file"]:
                    ext = Path(f).suffix
                    if ext:
                        lang = get_language_from_extension(ext)
                        lang_counts[lang] = lang_counts.get(lang, 0) + 1
                sorted_langs = sorted(lang_counts.items(), key=lambda item: item[1], reverse=True)
                data["lang_counts"] = pd.DataFrame(sorted_langs, columns=["Language", "Count"])
                logger.debug(f"Successfully fetched language counts on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for list_files also failed: {retry_e}")
                data["lang_counts"] = None
        except Exception as e:
            logger.warning(f"Error fetching language counts for {repo_name}: {e}")
            data["lang_counts"] = None
    except Exception as e:
        logger.warning(f"Error fetching language counts for {repo_name}: {e}")
        data["lang_counts"] = None

    # Fetch recent commits
    try:
        try:
            commits = repo.commit_history(limit=5, branch=repo.default_branch, force_refresh=force_refresh)
            if not commits.empty:
                commits["commit_date"] = commits.index.strftime("%Y-%m-%d %H:%M")
            data["commits"] = commits
            logger.debug(f"Fetched commit history for {repo_name}")
        except KeyError as ke:
            logger.warning(
                f"KeyError '{ke}' occurred in commit_history for {repo_name}. Retrying with force_refresh=True."
            )
            try:
                commits = repo.commit_history(limit=5, branch=repo.default_branch, force_refresh=True)
                if not commits.empty:
                    commits["commit_date"] = commits.index.strftime("%Y-%m-%d %H:%M")
                data["commits"] = commits
                logger.debug(f"Successfully fetched commit history on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for commit_history also failed: {retry_e}")
                data["commits"] = None
        except Exception as e:
            logger.warning(f"Error fetching commit history for {repo_name}: {e}")
            data["commits"] = None
    except Exception as e:
        logger.warning(f"Error fetching commit history for {repo_name}: {e}")
        data["commits"] = None

    # Fetch bus factor
    try:
        try:
            data["bus_factor"] = repo.bus_factor(force_refresh=force_refresh)
            logger.debug(f"Fetched bus factor for {repo_name}")
        except KeyError as ke:
            logger.warning(f"KeyError '{ke}' occurred in bus_factor for {repo_name}. Retrying with force_refresh=True.")
            try:
                data["bus_factor"] = repo.bus_factor(force_refresh=True)
                logger.debug(f"Successfully fetched bus factor on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for bus_factor also failed: {retry_e}")
                data["bus_factor"] = None
        except Exception as e:
            logger.warning(f"Error fetching bus factor for {repo_name}: {e}")
            data["bus_factor"] = None
    except Exception as e:
        logger.warning(f"Error fetching bus factor for {repo_name}: {e}")
        data["bus_factor"] = None

    # Fetch active branches (last 7 days)
    try:
        active_branches_list = []
        try:
            base_commits = repo.commit_history(limit=1, branch=repo.default_branch, force_refresh=force_refresh)
            if not base_commits.empty:
                cutoff_date = datetime.now(tz=base_commits.index.tz) - timedelta(days=7)
                all_branches = repo.branches(force_refresh=force_refresh)
                logger.debug(f"Checking {len(all_branches)} branches for recent activity in {repo_name}")
                for branch_name in all_branches["branch"]:
                    # Check if branch exists before attempting to get its history
                    if not repo.has_branch(branch_name):
                        logger.info(f"Skipping non-existent branch '{branch_name}' in {repo_name}")
                        continue

                    try:
                        branch_commits = repo.commit_history(branch=branch_name, limit=1, force_refresh=force_refresh)
                        if not branch_commits.empty and branch_commits.index[0] >= cutoff_date:
                            logger.debug(f"Branch '{branch_name}' is active in {repo_name}.")
                            try:
                                commit_date_str = branch_commits.index[0].strftime("%Y-%m-%d %H:%M")
                                active_branches_list.append(
                                    {
                                        "branch": branch_name,
                                        "last_commit_date": commit_date_str,
                                        "author": branch_commits.iloc[0]["author"],
                                    }
                                )
                            except (IndexError, AttributeError, KeyError) as e:
                                logger.warning(f"Error processing branch '{branch_name}' data: {e}")
                    except git.exc.GitCommandError as git_err:
                        logger.warning(f"Could not get history for branch '{branch_name}' in {repo_name}: {git_err}")
                        continue
                    except Exception as branch_err:
                        logger.warning(f"Unexpected error checking branch '{branch_name}' in {repo_name}: {branch_err}")
                        continue

                # Only try to sort if we have branches with valid data
                if active_branches_list:
                    data["active_branches"] = pd.DataFrame(active_branches_list).sort_values(
                        "last_commit_date", ascending=False
                    )
                else:
                    # Create empty DataFrame with expected columns
                    data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])
            else:
                logger.warning(f"No base commits found for {repo_name}, cannot determine active branches.")
                data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])  # Empty DF
        except KeyError as ke:
            logger.warning(
                f"KeyError '{ke}' occurred in active branches processing "
                f"for {repo_name}. Retrying with force_refresh=True."
            )
            try:
                active_branches_list = []
                base_commits = repo.commit_history(limit=1, branch=repo.default_branch, force_refresh=True)
                if not base_commits.empty:
                    cutoff_date = datetime.now(tz=base_commits.index.tz) - timedelta(days=7)
                    all_branches = repo.branches(force_refresh=True)
                    logger.debug(f"Retry: Checking {len(all_branches)} branches for recent activity in {repo_name}")
                    for branch_name in all_branches["branch"]:
                        # Check if branch exists before attempting to get its history
                        if not repo.has_branch(branch_name):
                            logger.info(f"Retry: Skipping non-existent branch '{branch_name}' in {repo_name}")
                            continue

                        try:
                            branch_commits = repo.commit_history(branch=branch_name, limit=1, force_refresh=True)
                            if not branch_commits.empty and branch_commits.index[0] >= cutoff_date:
                                logger.debug(f"Retry: Branch '{branch_name}' is active in {repo_name}.")
                                try:
                                    commit_date_str = branch_commits.index[0].strftime("%Y-%m-%d %H:%M")
                                    active_branches_list.append(
                                        {
                                            "branch": branch_name,
                                            "last_commit_date": commit_date_str,
                                            "author": branch_commits.iloc[0]["author"],
                                        }
                                    )
                                except (IndexError, AttributeError, KeyError) as e:
                                    logger.warning(f"Retry: Error processing branch '{branch_name}' data: {e}")
                        except Exception as branch_err:
                            logger.warning(f"Retry: Error checking branch '{branch_name}': {branch_err}")
                            continue

                    if active_branches_list:
                        data["active_branches"] = pd.DataFrame(active_branches_list).sort_values(
                            "last_commit_date", ascending=False
                        )
                    else:
                        data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])
                else:
                    data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])
                logger.debug(f"Successfully processed active branches on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for active branches also failed: {retry_e}")
                data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])
        except Exception as e:
            logger.warning(f"Error processing active branches for {repo_name}: {e}")
            data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])

        logger.debug(f"Finished checking active branches for {repo_name}")
    except Exception as e:
        logger.exception(f"Error fetching active branches for {repo_name}: {e}")
        data["active_branches"] = pd.DataFrame(columns=["branch", "last_commit_date", "author"])  # Empty DF on error

    logger.info(f"Finished fetching overview data for {repo_name}")
    # Return dict with data and timestamp
    return {"data": data, "refreshed_at": datetime.now()}


def fetch_code_health_data(repo: Repository, force_refresh=False):
    """Worker function to fetch all data needed for the Code Health tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching code health data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        try:
            has_cov = repo.has_coverage()
            logger.debug(f"Coverage available for {repo_name}: {has_cov}")
            coverage_df = repo.coverage() if has_cov else pd.DataFrame(columns=["filename", "coverage"])
            logger.debug(f"Fetched coverage data for {repo_name}")
            change_rates_df = repo.file_change_rates(
                days=7, coverage=False, branch=repo.default_branch, force_refresh=force_refresh
            )
            logger.debug(f"Fetched change rates for {repo_name}")

            # Get file details and ensure datetime columns are properly handled
            file_details_df = repo.file_detail(force_refresh=force_refresh)
            if not file_details_df.empty and "last_edit_date" in file_details_df.columns:
                try:
                    # Convert to datetime with UTC timezone
                    file_details_df["last_edit_date"] = pd.to_datetime(file_details_df["last_edit_date"], utc=True)
                except Exception as e:
                    logger.warning(f"Error converting last_edit_date to datetime: {e}")
                    file_details_df["last_edit_date"] = pd.NaT
            logger.debug(f"Fetched file details for {repo_name}")

            coverage_df = coverage_df.rename(columns={"filename": "file"}).set_index("file")
            change_rates_df = change_rates_df.rename_axis("file")
            file_details_df = file_details_df.rename_axis("file")

            merged = pd.merge(file_details_df, change_rates_df, on="file", how="outer")
            merged_data = pd.merge(merged, coverage_df, on="file", how="outer")
            data["merged_data"] = merged_data  # Pass DataFrame
            logger.debug(f"Merged health data for {repo_name}, shape: {merged_data.shape}")

            overall_coverage = "N/A"
            if has_cov and "lines_covered" in merged_data.columns and "total_lines" in merged_data.columns:
                total_lines = merged_data["total_lines"].fillna(0).sum()
                if total_lines > 0:
                    overall_coverage = f"{merged_data['lines_covered'].fillna(0).sum() / total_lines:.1%}"
                else:
                    overall_coverage = "0.0%"
            elif has_cov and not merged_data.empty and "coverage" in merged_data.columns:
                mean_cov = merged_data["coverage"].mean()
                overall_coverage = f"{mean_cov:.1%}" if pd.notna(mean_cov) else "N/A"
            data["overall_coverage"] = overall_coverage
            logger.debug(f"Calculated overall coverage for {repo_name}: {overall_coverage}")

            avg_edit_rate = "N/A"
            if "edit_rate" in merged_data.columns:
                median_rate = merged_data["edit_rate"].fillna(0).median()
                avg_edit_rate = f"{median_rate:.2f}"
            data["avg_edit_rate"] = avg_edit_rate
            logger.debug(f"Calculated avg edit rate for {repo_name}: {avg_edit_rate}")
        except KeyError as ke:
            logger.warning(
                f"KeyError '{ke}' occurred in code health data for {repo_name}. Retrying with force_refresh=True."
            )
            try:
                # Retry all operations with force_refresh=True
                has_cov = repo.has_coverage()
                coverage_df = repo.coverage() if has_cov else pd.DataFrame(columns=["filename", "coverage"])
                change_rates_df = repo.file_change_rates(
                    days=7, coverage=False, branch=repo.default_branch, force_refresh=True
                )
                file_details_df = repo.file_detail(force_refresh=True)

                if not file_details_df.empty and "last_edit_date" in file_details_df.columns:
                    try:
                        file_details_df["last_edit_date"] = pd.to_datetime(file_details_df["last_edit_date"], utc=True)
                    except Exception as e:
                        logger.warning(f"Retry: Error converting last_edit_date to datetime: {e}")
                        file_details_df["last_edit_date"] = pd.NaT

                coverage_df = coverage_df.rename(columns={"filename": "file"}).set_index("file")
                change_rates_df = change_rates_df.rename_axis("file")
                file_details_df = file_details_df.rename_axis("file")

                merged = pd.merge(file_details_df, change_rates_df, on="file", how="outer")
                merged_data = pd.merge(merged, coverage_df, on="file", how="outer")
                data["merged_data"] = merged_data

                # Calculate metrics
                overall_coverage = "N/A"
                if has_cov and "lines_covered" in merged_data.columns and "total_lines" in merged_data.columns:
                    total_lines = merged_data["total_lines"].fillna(0).sum()
                    if total_lines > 0:
                        overall_coverage = f"{merged_data['lines_covered'].fillna(0).sum() / total_lines:.1%}"
                    else:
                        overall_coverage = "0.0%"
                elif has_cov and not merged_data.empty and "coverage" in merged_data.columns:
                    mean_cov = merged_data["coverage"].mean()
                    overall_coverage = f"{mean_cov:.1%}" if pd.notna(mean_cov) else "N/A"
                data["overall_coverage"] = overall_coverage

                avg_edit_rate = "N/A"
                if "edit_rate" in merged_data.columns:
                    median_rate = merged_data["edit_rate"].fillna(0).median()
                    avg_edit_rate = f"{median_rate:.2f}"
                data["avg_edit_rate"] = avg_edit_rate

                logger.debug(
                    f"Successfully fetched code health data on retry for {repo_name}. "
                    f"Shape: {merged_data.shape if hasattr(merged_data, 'shape') else 'N/A'}"
                )
            except Exception as retry_e:
                logger.warning(f"Retry for code health data also failed: {retry_e}")
                data["merged_data"] = None
                data["overall_coverage"] = "Error"
                data["avg_edit_rate"] = "Error"
        except Exception as e:
            logger.exception(f"Error fetching code health data for {repo_name}: {e}")
            data["merged_data"] = None
            data["overall_coverage"] = "Error"
            data["avg_edit_rate"] = "Error"
    except Exception as e:
        logger.exception(f"Error fetching code health data for {repo_name}: {e}")
        data["merged_data"] = None
        data["overall_coverage"] = "Error"
        # Ensure avg_edit_rate is also set in error case if it was calculated before error
        data.setdefault("avg_edit_rate", "Error")

    logger.info(f"Finished fetching code health data for {repo_name}")
    # Return dict with data and timestamp
    return {"data": data, "refreshed_at": datetime.now()}


def fetch_contributor_data(repo: Repository, force_refresh=False):
    """Worker function to fetch data for Contributor Patterns tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching contributor data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        try:
            hours_df = repo.hours_estimate(branch=repo.default_branch, committer=False, force_refresh=force_refresh)
            data["hours"] = hours_df  # Pass DataFrame
            logger.debug(f"Fetched hours estimate for {repo_name}")
        except KeyError as ke:
            logger.warning(
                f"KeyError '{ke}' occurred in hours_estimate for {repo_name}. Retrying with force_refresh=True."
            )
            try:
                hours_df = repo.hours_estimate(branch=repo.default_branch, committer=False, force_refresh=True)
                data["hours"] = hours_df  # Pass DataFrame
                logger.debug(f"Successfully fetched hours estimate on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for hours estimate also failed: {retry_e}")
                data["hours"] = None
        except Exception as e:
            logger.warning(f"Error fetching hours estimate for {repo_name}: {e}")
            data["hours"] = None
    except Exception as e:
        logger.warning(f"Error fetching hours estimate for {repo_name}: {e}")
        data["hours"] = None

    logger.info(f"Finished fetching contributor data for {repo_name}")
    # Return dict with data and timestamp
    return {"data": data, "refreshed_at": datetime.now()}


def fetch_tags_data(repo: Repository, force_refresh=False):
    """Worker function to fetch data for Tags tab."""
    repo_name = repo.repo_name
    logger.info(f"Fetching tags data for {repo_name} (force_refresh={force_refresh})")
    data = {}
    try:
        try:
            # First attempt without skip_broken to match test expectations
            tags_df = repo.tags(force_refresh=force_refresh)
            data["tags"] = tags_df  # Pass DataFrame
            logger.debug(f"Fetched tags data for {repo_name}")
        except KeyError as ke:
            logger.warning(f"KeyError '{ke}' occurred in tags for {repo_name}. Retrying with force_refresh=True.")
            try:
                # Retry with skip_broken=True explicitly
                tags_df = repo.tags(force_refresh=True, skip_broken=True)
                data["tags"] = tags_df  # Pass DataFrame
                logger.debug(f"Successfully fetched tags data on retry for {repo_name}")
            except Exception as retry_e:
                logger.warning(f"Retry for tags also failed: {retry_e}")
                data["tags"] = None
        except ValueError as ve:
            # Special handling for file read errors
            if "read of closed file" in str(ve):
                logger.warning(
                    f"File handle error in tags for {repo_name}: {ve}. "
                    f"Retrying with force_refresh=True and skip_broken=True."
                )
                try:
                    # Try again with both force_refresh and skip_broken
                    tags_df = repo.tags(force_refresh=True, skip_broken=True)
                    data["tags"] = tags_df
                    logger.debug(f"Successfully fetched tags data with skip_broken=True for {repo_name}")
                except Exception as retry_e:
                    logger.warning(f"Retry for tags with skip_broken also failed: {retry_e}")
                    # Last resort: try to create a minimal empty dataframe with expected columns
                    data["tags"] = pd.DataFrame(columns=["tag", "date", "message", "author"])
            else:
                # Re-raise other ValueErrors for the general exception handler
                raise
        except git.exc.GitCommandError as ge:
            # Special handling for git command errors
            error_msg = str(ge)
            if "unknown object type" in error_msg or "could not be resolved" in error_msg or "bad file" in error_msg:
                logger.warning(
                    f"Git error handling tags for {repo_name}: {ge}. "
                    f"Retrying with force_refresh=True and skip_broken=True."
                )
                try:
                    # Try again with both force_refresh and skip_broken to handle corrupted tags
                    tags_df = repo.tags(force_refresh=True, skip_broken=True)
                    data["tags"] = tags_df
                    logger.debug(f"Successfully fetched tags data with skip_broken=True for {repo_name}")
                except Exception as retry_e:
                    logger.warning(f"Retry for tags with skip_broken also failed: {retry_e}")
                    # Last resort: try to create a minimal empty dataframe with expected columns
                    data["tags"] = pd.DataFrame(columns=["tag", "date", "message", "author"])
            else:
                # Re-raise other GitCommandErrors for the general exception handler
                raise
        except Exception as e:
            logger.warning(f"Error fetching tags data for {repo_name}: {e}")
            data["tags"] = None
    except Exception as e:
        logger.warning(f"Error fetching tags data for {repo_name}: {e}")
        data["tags"] = None

    logger.info(f"Finished fetching tags data for {repo_name}")
    # Return dict with data and timestamp
    return {"data": data, "refreshed_at": datetime.now()}


def fetch_cumulative_blame_data(repo: Repository, force_refresh=False):
    """Worker function to fetch data for Cumulative Blame tab."""
    repo_name = repo.repo_name
    commit_limit = 100  # Explicitly limit commits processed
    logger.info(
        f"Fetching cumulative blame data for {repo_name} (force_refresh={force_refresh}, commit_limit={commit_limit})"
    )
    data = {}
    blame_df = None  # Initialize blame_df
    try:
        kwargs = {
            "committer": False,
            "limit": commit_limit,  # Pass the commit limit
            "skip_broken": True,  # Always use skip_broken=True for robustness
            # Removed force_refresh=True - let gitpandas handle cache via decorator/backend
        }
        logger.debug(f"Calling repo.cumulative_blame for {repo_name} with kwargs: {kwargs}")
        start_time = time.time()
        # Pass force_refresh to the method if it accepts it, otherwise rely on cache backend
        # Assuming cumulative_blame uses the @multicache decorator which respects force_refresh if passed
        # Let's try passing it explicitly
        try:
            blame_df = repo.cumulative_blame(force_refresh=force_refresh, **kwargs)
        except KeyError as ke:
            # Specifically handle KeyError on 'rev' which can happen in gitpandas
            if "rev" in str(ke):
                logger.warning(
                    f"KeyError 'rev' occurred in cumulative_blame for {repo_name}. This is likely a gitpandas issue."
                )
                # Try once more with force_refresh=True to bypass cache
                logger.info(f"Retrying cumulative_blame for {repo_name} with force_refresh=True")
                try:
                    blame_df = repo.cumulative_blame(force_refresh=True, **kwargs)
                except Exception as retry_e:
                    logger.exception(f"Retry also failed with: {retry_e}")
                    blame_df = None
            else:
                # Re-raise other KeyErrors
                raise
        except git.exc.GitCommandError as ge:
            # Handle git command errors
            logger.warning(
                f"Git error in cumulative_blame for {repo_name}: {ge}. "
                f"Using skip_broken=True should handle this."
            )
            blame_df = None
        except ValueError as ve:
            # Handle value errors
            logger.warning(f"Value error in cumulative_blame for {repo_name}: {ve}")
            blame_df = None
        except Exception as e:
            logger.exception(f"Unexpected error in cumulative_blame for {repo_name}: {e}")
            blame_df = None

        end_time = time.time()
        logger.info(f"repo.cumulative_blame call completed in {end_time - start_time:.2f} seconds for {repo_name}")

        # --- Detailed Logging of Result --- #
        if blame_df is None:
            logger.warning(f"repo.cumulative_blame returned None for {repo_name}")
        elif not isinstance(blame_df, pd.DataFrame):
            logger.warning(f"repo.cumulative_blame returned type {type(blame_df)}, expected DataFrame, for {repo_name}")
        elif blame_df.empty:
            logger.warning(f"repo.cumulative_blame returned an empty DataFrame for {repo_name}")
        else:
            logger.debug(f"Fetched cumulative blame DataFrame shape: {blame_df.shape} for {repo_name}")
            logger.debug(f"DataFrame columns: {blame_df.columns.tolist()}")
            logger.debug(f"DataFrame index type: {type(blame_df.index)}")

            # Store the result (or None if issues occurred)
            data["cumulative_blame"] = blame_df

    except Exception as e:
        # Log the specific error during the call
        logger.exception(f"Error during repo.cumulative_blame call for {repo_name}: {e}")
        data["cumulative_blame"] = None  # Ensure it's None on error

    logger.info(f"Finished fetch_cumulative_blame_data function for {repo_name}. Returning data dict.")
    logger.debug(
        f"Returning data structure: {{ 'data': {{ 'cumulative_blame': "
        f"type {type(data.get('cumulative_blame'))} }}, 'refreshed_at': ... }}"
    )
    # Return dict with data and timestamp
    return {"data": data, "refreshed_at": datetime.now()}


# --- End Data Fetching Functions --- #


def _load_repo_detail(self, repo_path, force_refresh=False):
    repo = self._get_repo_instance(repo_path, force_refresh)
    if repo is None:
        return None, None, None

    commit_history = self._fetch_with_cache(repo, "commit_history", force_refresh)
    branch_info = self._fetch_with_cache(repo, "branches", force_refresh)
    tag_info = self._fetch_with_cache(repo, "tags", force_refresh)
    return commit_history, branch_info, tag_info


def _fetch_cumulative_blame_data(self, repo_path, force_refresh=False):
    """Fetches cumulative blame data, using cache unless forced."""
    logger.info(f"Starting cumulative blame data fetch for repo: {repo_path} (force_refresh={force_refresh})")

    repo = self._get_repo_instance(repo_path, force_refresh)
    if repo is None:
        logger.error(f"Failed to get Repository instance for {repo_path}")
        return {"data": {"blame": None}, "refreshed_at": datetime.now()}

    logger.info(f"Successfully got Repository instance for {repo_path}, default branch: {repo.default_branch}")

    # Use a fixed number of datapoints for performance and set committer=False
    kwargs = {"num_datapoints": 50, "committer": False}
    logger.info(f"Attempting to fetch cumulative blame with kwargs: {kwargs}")

    try:
        start_time = time.time()
        blame_data = self._fetch_with_cache(repo, "cumulative_blame", force_refresh, **kwargs)
        end_time = time.time()

        if blame_data is None:
            logger.error(f"Cumulative blame data fetch returned None for {repo_path}")
            return {"data": {"blame": None}, "refreshed_at": datetime.now()}

        logger.info(f"Successfully fetched cumulative blame data in {end_time - start_time:.2f} seconds")
        logger.debug(f"Blame data shape: {blame_data.shape if hasattr(blame_data, 'shape') else 'N/A'}")
        logger.debug(f"Blame data columns: {list(blame_data.columns) if hasattr(blame_data, 'columns') else 'N/A'}")

        return {"data": {"blame": blame_data}, "refreshed_at": datetime.now()}
    except Exception as e:
        logger.exception(f"Exception while fetching cumulative blame data for {repo_path}: {str(e)}")
        return {"data": {"blame": None}, "refreshed_at": datetime.now()}


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
    logger.info(f"Starting async cumulative blame fetch for repo: {repo_path}")

    worker = CumulativeBlameWorker(self._fetch_cumulative_blame_data, repo_path, force_refresh)
    worker.signals.result.connect(lambda path, data, ts: self._handle_cumulative_blame_result(path, data, ts))
    worker.signals.finished.connect(self._worker_complete)
    worker.signals.error.connect(self._worker_error)

    logger.debug(f"Created CumulativeBlameWorker for {repo_path}")
    self.threadpool.start(worker)
    self.active_workers += 1
    self.check_global_loading_state()
    logger.info(f"Started cumulative blame worker for {repo_path}, active workers: {self.active_workers}")


# --- Signals for UI updates (to be connected in MainWindow) ---
repo_detail_fetched = Signal(str, object, object, object, datetime)  # repo_path, history, branches, tags, timestamp


# --- Worker Signals --- #
class WorkerSignals(QObject):
    """Worker signals for asynchronous operations."""
    result = Signal(str, object, datetime)  # repo_path, data, timestamp
    finished = Signal(str)  # worker_id
    error = Signal(str, str)  # worker_id, error message


# --- Worker Base Class --- #
class Worker(QRunnable):
    def __init__(self, fn, repo_path, force_refresh):
        super().__init__()
        self.fn = fn
        self.repo_path = repo_path
        self.force_refresh = force_refresh
        self.signals = WorkerSignals()
        self.worker_id = f"{fn.__name__}_{repo_path}_{time.time()}"  # Unique ID

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
    repo_detail_fetched = Signal(str, object, datetime)  # repo_path, (history, branches, tags), timestamp
    contributors_data_fetched = Signal(str, object, datetime)  # repo_path, data_dict, timestamp
    code_health_data_fetched = Signal(str, object, datetime)  # repo_path, data_dict, timestamp
    tags_data_fetched = Signal(str, object, datetime)  # repo_path, tags_df, timestamp
    cumulative_blame_data_fetched = Signal(str, object, datetime)  # repo_path, blame_df, timestamp
    global_loading_changed = Signal(bool)  # True if loading, False if idle
    repo_instantiation_failed = Signal(str)  # repo_path - Signal if repo fails basic load

    def __init__(self):
        super().__init__()
        self.threadpool = QThreadPool()
        logger.info(f"Multithreading with maximum {self.threadpool.maxThreadCount()} threads")
        self.repo_cache = {}  # Cache Repository objects
        self.active_workers = 0

    def _get_repo_instance(self, repo_path, force_refresh=False):
        if not force_refresh and repo_path in self.repo_cache:
            logger.debug(f"Using cached Repository instance for: {repo_path}")
            return self.repo_cache[repo_path]
        try:
            logger.debug(f"Creating new Repository instance for: {repo_path}")
            repo = Repository(working_dir=repo_path, verbose=False)  # Add cache_backend later if needed
            self.repo_cache[repo_path] = repo
            return repo
        except Exception as e:
            logger.error(f"Error creating Repository instance for {repo_path}: {e}")
            traceback.print_exc()
            return None

    def _fetch_with_cache(self, repo: Repository, method_name: str, force_refresh: bool, **kwargs):
        """Generic fetch method using gitpandas caching if enabled and not forced."""
        if repo is None:
            return None
        try:
            method_to_call = getattr(repo, method_name)
            logger.debug(
                f"Calling {method_name} for {repo.repo_name} "
                f"(Force Refresh: {force_refresh}, Args: {kwargs})..."
            )
            start = time.time()
            data = method_to_call(**kwargs)
            end = time.time()
            logger.debug(f"Finished {method_name} for {repo.repo_name} in {end - start:.2f} seconds.")
            return data
        except Exception as e:
            logger.error(f"Error fetching {method_name} for {repo.repo_name}: {e}")
            traceback.print_exc()
            return None

    # --- Internal Data Aggregation/Loading Functions --- #
    def _load_repo_detail(self, repo_path, force_refresh=False):
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None:
            return None, None, None

        # Fetch data using the generic helper
        commit_history = self._fetch_with_cache(repo, "commit_history", force_refresh)
        branch_info = self._fetch_with_cache(repo, "branches", force_refresh)
        tag_info = self._fetch_with_cache(repo, "tags", force_refresh)

        return commit_history, branch_info, tag_info

    def _fetch_contributors_data(self, repo_path, force_refresh=False):
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None:
            return None
        hours_df = self._fetch_with_cache(repo, "hours_estimate", force_refresh, committer=True)
        bus_factor_df = self._fetch_with_cache(repo, "bus_factor", force_refresh)
        return {"hours": hours_df, "bus_factor": bus_factor_df}

    def _fetch_code_health_data(self, repo_path, force_refresh=False):
        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None:
            return None
        file_details = self._fetch_with_cache(repo, "file_detail", force_refresh)
        coverage_df = self._fetch_with_cache(repo, "coverage", force_refresh)
        change_rates = self._fetch_with_cache(repo, "file_change_rates", force_refresh)

        # Merge data (handle potentially missing data)
        merged_data = pd.DataFrame()
        if file_details is not None:
            merged_data = file_details
        if change_rates is not None:
            # file_details index is 'file', change_rates index is 'filename'
            change_rates = change_rates.reset_index().rename(columns={"filename": "file"}).set_index("file")
            merged_data = pd.merge(merged_data, change_rates, left_index=True, right_index=True, how="outer")
        if coverage_df is not None and not coverage_df.empty:
            # coverage index is 'filename'
            coverage_df = coverage_df.reset_index().rename(columns={"filename": "file"}).set_index("file")
            merged_data = pd.merge(
                merged_data, coverage_df[["coverage"]], left_index=True, right_index=True, how="left"
            )
        else:
            merged_data["coverage"] = pd.NA

        # Calculate overall coverage
        overall_coverage = "N/A"
        if (
            coverage_df is not None
            and not coverage_df.empty
            and "lines_covered" in coverage_df.columns
            and "total_lines" in coverage_df.columns
        ):
            total_covered = coverage_df["lines_covered"].sum()
            total_lines = coverage_df["total_lines"].sum()
            if total_lines > 0:
                overall_coverage = f"{(total_covered / total_lines):.1%}"

        return {"merged_data": merged_data, "overall_coverage": overall_coverage}

    def _fetch_cumulative_blame_data(self, repo_path, force_refresh=False):
        """Fetches cumulative blame data, using cache unless forced."""
        logger.info(f"Starting cumulative blame data fetch for repo: {repo_path} (force_refresh={force_refresh})")

        repo = self._get_repo_instance(repo_path, force_refresh)
        if repo is None:
            logger.error(f"Failed to get Repository instance for {repo_path}")
            return {"data": {"blame": None}, "refreshed_at": datetime.now()}

        logger.info(f"Successfully got Repository instance for {repo_path}, default branch: {repo.default_branch}")

        # Use a fixed number of datapoints for performance and set committer=False
        kwargs = {"num_datapoints": 50, "committer": False}
        logger.info(f"Attempting to fetch cumulative blame with kwargs: {kwargs}")

        try:
            start_time = time.time()
            blame_data = self._fetch_with_cache(repo, "cumulative_blame", force_refresh, **kwargs)
            end_time = time.time()

            if blame_data is None:
                logger.error(f"Cumulative blame data fetch returned None for {repo_path}")
                return {"data": {"blame": None}, "refreshed_at": datetime.now()}

            logger.info(f"Successfully fetched cumulative blame data in {end_time - start_time:.2f} seconds")
            logger.debug(f"Blame data shape: {blame_data.shape if hasattr(blame_data, 'shape') else 'N/A'}")
            logger.debug(f"Blame data columns: {list(blame_data.columns) if hasattr(blame_data, 'columns') else 'N/A'}")

            return {"data": {"blame": blame_data}, "refreshed_at": datetime.now()}
        except Exception as e:
            logger.exception(f"Exception while fetching cumulative blame data for {repo_path}: {str(e)}")
            return {"data": {"blame": None}, "refreshed_at": datetime.now()}

    # --- Public Fetch Methods ---
    def fetch_repo_detail_async(self, repo_path, force_refresh=False):
        """Fetches basic repo details (history, branches, tags) asynchronously."""
        worker = RepoDetailWorker(self._load_repo_detail, repo_path, force_refresh)
        # Connect result signal specifically for RepoDetailWorker
        worker.signals.result.connect(
            lambda path, result_tuple, ts: self.repo_detail_fetched.emit(path, result_tuple, ts)
        )
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
        logger.info(f"Starting async cumulative blame fetch for repo: {repo_path}")

        worker = CumulativeBlameWorker(self._fetch_cumulative_blame_data, repo_path, force_refresh)
        worker.signals.result.connect(lambda path, data, ts: self._handle_cumulative_blame_result(path, data, ts))
        worker.signals.finished.connect(self._worker_complete)
        worker.signals.error.connect(self._worker_error)

        logger.debug(f"Created CumulativeBlameWorker for {repo_path}")
        self.threadpool.start(worker)
        self.active_workers += 1
        self.check_global_loading_state()
        logger.info(f"Started cumulative blame worker for {repo_path}, active workers: {self.active_workers}")

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

    def _handle_cumulative_blame_result(self, repo_path, blame_data, timestamp):
        """Handle the result of cumulative blame data fetch."""
        if blame_data is None:
            logger.warning(f"Received None blame data for {repo_path}")
        else:
            logger.info(f"Received valid blame data for {repo_path} at {timestamp}")
            logger.debug(f"Blame data summary: Shape={blame_data.shape if hasattr(blame_data, 'shape') else 'N/A'}")

        self.cumulative_blame_data_fetched.emit(repo_path, blame_data, timestamp)
        logger.info(f"Emitted cumulative_blame_data_fetched signal for {repo_path}")
