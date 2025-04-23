import sys
import traceback
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import git # For exception handling

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
        file_details_df = repo.file_detail()
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

# --- End Data Fetching Functions --- # 