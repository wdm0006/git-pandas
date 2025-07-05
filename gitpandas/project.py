"""
.. module:: projectdirectory
   :platform: Unix, Windows
   :synopsis: A module for examining collections of git repositories as a whole

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

import math
import os
import sys
import warnings

import numpy as np
import pandas as pd
import requests
from git import GitCommandError

from gitpandas.logging import logger  # Import the logger
from gitpandas.repository import Repository

try:
    from joblib import Parallel, delayed

    _has_joblib = True
except ImportError:
    _has_joblib = False

__author__ = "willmcginnis"


# Functions for joblib.
def _branches_func(r):
    return r.branches()


def _revs_func(repo, branch, limit, skip, num_datapoints):
    revs = repo.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints)
    revs["repository"] = repo.repo_name
    return revs


def _tags_func(repo):
    return repo.tags()


class ProjectDirectory:
    """A class for analyzing multiple git repositories in a directory or from explicit paths.

    This class provides functionality to analyze multiple git repositories together, whether they are
    local repositories in a directory, explicitly specified local repositories, or remote repositories
    that need to be cloned. It offers methods for analyzing commit history, blame information,
    file changes, and other git metrics across all repositories.

    Args:
        working_dir (Union[str, List[str], None]): The source of repositories to analyze:
            - If None: Uses current working directory to find repositories
            - If str: Path to directory containing git repositories
            - If List[str]: List of paths to git repositories or Repository instances
        ignore_repos (Optional[List[str]]): List of repository names to ignore
        verbose (bool, optional): Whether to print verbose output. Defaults to True.
        tmp_dir (Optional[str]): Directory to clone remote repositories into. Created if not provided.
        cache_backend (Optional[object]): Cache backend instance from gitpandas.cache
        default_branch (str, optional): Name of the default branch to use. Defaults to 'main'.

    Attributes:
        repo_dirs (Union[set, list]): Set of repository directories or list of Repository instances
        repos (List[Repository]): List of Repository objects being analyzed

    Examples:
        >>> # Create from directory containing repos
        >>> pd = ProjectDirectory(working_dir='/path/to/repos')

        >>> # Create from explicit local repos
        >>> pd = ProjectDirectory(working_dir=['/path/to/repo1', '/path/to/repo2'])

        >>> # Create from remote repos
        >>> pd = ProjectDirectory(working_dir=['git://github.com/user/repo.git'])

    Note:
        When using remote repositories, they will be cloned to temporary directories.
        This can be slow for large repositories.
    """

    def __init__(
        self,
        working_dir=None,
        ignore_repos=None,
        verbose=True,
        tmp_dir=None,
        cache_backend=None,
        default_branch="main",
    ):
        """Initialize a ProjectDirectory instance.

        Args:
            working_dir (Union[str, List[str], None]): The source of repositories to analyze:
                - If None: Uses current working directory to find repositories
                - If str: Path to directory containing git repositories
                - If List[str]: List of paths to git repositories or Repository instances
            ignore_repos (Optional[List[str]]): List of repository names to ignore
            verbose (bool, optional): Whether to print verbose output. Defaults to True.
            tmp_dir (Optional[str]): Directory to clone remote repositories into. Created if not provided.
            cache_backend (Optional[object]): Cache backend instance from gitpandas.cache
            default_branch (str, optional): Name of the default branch to use. Defaults to 'main'.
        """
        logger.info(f"Initializing ProjectDirectory with working_dir={working_dir}, ignore_repos={ignore_repos}")
        # First get all potential repository paths
        if working_dir is None:
            # When no working_dir is provided, look for git repos in current directory
            potential_repos = {x[0].split(".git")[0] for x in os.walk(os.getcwd()) if ".git" in x[0]}
            self.repo_dirs = {path for path in potential_repos if self._is_valid_git_repo(path)}
        elif isinstance(working_dir, list):
            # For list input, keep Repository instances and validate paths
            self.repo_dirs = []
            for r in working_dir:
                if isinstance(r, Repository):
                    self.repo_dirs.append(r)
                elif isinstance(r, str):
                    # For URLs, add them directly as they'll be cloned later
                    if r.startswith(("git://", "https://", "http://")) or self._is_valid_git_repo(r):
                        self.repo_dirs.append(r)
                    elif verbose:
                        # Use logger instead of print
                        logger.warning(f"Skipping invalid git repository at {r}")
        else:
            # When working_dir is a directory path, look for git repos in it
            potential_repos = {x[0].split(".git")[0] for x in os.walk(working_dir) if ".git" in x[0]}
            self.repo_dirs = {path for path in potential_repos if self._is_valid_git_repo(path)}

        # If we already have Repository instances, use them directly
        if all(isinstance(r, Repository) for r in self.repo_dirs):
            # Filter Repository instances by repo_name if ignore_repos is specified
            if ignore_repos is not None:
                self.repos = [r for r in self.repo_dirs if r.repo_name not in ignore_repos]
            else:
                self.repos = self.repo_dirs
        else:
            # For paths, filter before creating Repository objects
            if ignore_repos is not None:
                # Filter paths by repository name before creating any Repository objects
                self.repo_dirs = [r for r in self.repo_dirs if self._get_repo_name_from_path(r) not in ignore_repos]

            # Now create Repository objects only for the filtered paths
            self.repos = []
            for r in self.repo_dirs:
                try:
                    repo = Repository(
                        r, verbose=verbose, tmp_dir=tmp_dir, cache_backend=cache_backend, default_branch=default_branch
                    )
                    self.repos.append(repo)
                except (GitCommandError, ValueError, OSError) as e:
                    # Skip invalid repositories
                    if verbose:
                        # Use logger instead of print
                        logger.warning(f"Could not initialize repository at {r}: {str(e)}")

        self.default_branch = default_branch
        logger.info(f"Initialized ProjectDirectory with {len(self.repos)} repositories.")

    def _repo_name(self):
        warnings.warn(
            "please use repo_name() now instead of _repo_name()",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.repo_name()

    def repo_name(self):
        """Returns a DataFrame containing the names of all repositories in the project.

        Returns:
            pandas.DataFrame: A DataFrame with a single column:
                - repository (str): Name of each repository
        """
        logger.info("Generating repository name DataFrame.")
        ds = [[x.repo_name] for x in self.repos]
        df = pd.DataFrame(ds, columns=["repository"])
        logger.debug("Generated repository name DataFrame.")
        return df

    def is_bare(self):
        """
        Returns a dataframe of repo names and whether or not they are bare.

        :return: DataFrame
        """
        logger.info("Generating is_bare DataFrame.")
        ds = [[x.repo_name, x.is_bare()] for x in self.repos]
        df = pd.DataFrame(ds, columns=["repository", "is_bare"])
        logger.debug("Generated is_bare DataFrame.")
        return df

    def has_coverage(self):
        """
        Returns a DataFrame of repo names and whether or not they have a .coverage file that can be parsed

        :return: DataFrame
        """
        logger.info("Generating has_coverage DataFrame.")
        ds = [[x.repo_name, x.has_coverage()] for x in self.repos]
        df = pd.DataFrame(ds, columns=["repository", "has_coverage"])
        logger.debug("Generated has_coverage DataFrame.")
        return df

    def coverage(self):
        """
        Will return a DataFrame with coverage information (if available) for each repo in the project).

        If there is a .coverage file available, this will attempt to form a DataFrame with that information in it, which
        will contain the columns:

         * repository
         * filename
         * lines_covered
         * total_lines
         * coverage

        If it can't be found or parsed, an empty DataFrame of that form will be returned.

        :return: DataFrame
        """
        logger.info("Generating coverage report for project.")
        df = pd.DataFrame(
            columns=[
                "filename",
                "lines_covered",
                "total_lines",
                "coverage",
                "repository",
            ]
        )

        for repo in self.repos:
            try:
                cov = repo.coverage()
                if not cov.empty:
                    cov = cov.copy()  # Avoid SettingWithCopyWarning
                    cov["repository"] = repo.repo_name
                    df = pd.concat([df, cov], ignore_index=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} seems to not have coverage")

        df = df.reset_index(drop=True)
        logger.info(f"Generated coverage report with {len(df)} rows.")
        return df

    def file_change_rates(
        self,
        branch=None,
        limit=None,
        coverage=False,
        days=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Will return a DataFrame containing some basic aggregations of the file change history data,
        and optionally test coverage data from a coverage_data.py .coverage file. The aim here is to
        identify files in the project which have abnormal edit rates, or the rate of changes without
        growing the files size. If a file has a high change rate and poor test coverage, then it is
        a great candidate for writing more tests.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to return, None for no limit
            coverage (bool, optional): Whether to include coverage data. Defaults to False.
            days (Optional[int]): Number of days to return if limit is None
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with file change statistics and optionally coverage data
        """
        logger.info(f"Calculating file change rates for branch '{branch or self.default_branch}'.")
        if branch is None:
            branch = self.default_branch

        columns = [
            "file",
            "unique_committers",
            "abs_rate_of_change",
            "net_rate_of_change",
            "net_change",
            "abs_change",
            "edit_rate",
            "lines",
            "repository",
        ]
        if coverage:
            columns += ["lines_covered", "total_lines", "coverage"]

        # Initialize empty DataFrame with all required columns
        df = None

        for repo in self.repos:
            try:
                fcr = repo.file_change_rates(
                    branch=branch,
                    limit=limit,
                    coverage=coverage,
                    days=days,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                if not fcr.empty:
                    fcr = fcr.copy()  # Avoid SettingWithCopyWarning
                    fcr["repository"] = repo.repo_name
                    df = fcr if df is None else pd.concat([df, fcr], ignore_index=True, sort=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} seems to not have the branch: {branch}")

        if df is None:
            # If no data was collected, return empty DataFrame with correct columns
            df = pd.DataFrame(columns=columns)

        # Ensure consistent column order and reset index
        df = df[columns]
        df = df.reset_index(drop=True)
        logger.info(f"Calculated file change rates with {len(df)} rows.")
        return df

    def hours_estimate(
        self,
        branch=None,
        grouping_window=0.5,
        single_commit_hours=0.5,
        limit=None,
        days=None,
        committer=True,
        by=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Returns a DataFrame containing the estimated hours spent by each committer/author.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            grouping_window (float, optional): Hours threshold for considering commits part of same session.
                Defaults to 0.5.
            single_commit_hours (float, optional): Hours to assign to single commits. Defaults to 0.5.
            limit (Optional[int]): Maximum number of commits to analyze
            days (Optional[int]): If provided, only analyze commits from last N days
            committer (bool, optional): If True use committer, if False use author. Defaults to True.
            by (Optional[str]): How to group results. One of None, 'committer', 'author'
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with hours estimates
        """
        logger.info(f"Estimating hours for branch '{branch or self.default_branch}'.")
        if branch is None:
            branch = self.default_branch

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        com = "committer" if committer else "author"

        df = pd.DataFrame(columns=[com, "hours", "repository"])

        for repo in self.repos:
            try:
                ch = repo.hours_estimate(
                    branch=branch,
                    grouping_window=grouping_window,
                    single_commit_hours=single_commit_hours,
                    limit=limit,
                    days=days,
                    committer=committer,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                if not ch.empty:
                    ch = ch.copy()  # Avoid SettingWithCopyWarning
                    ch["repository"] = repo.repo_name
                    # Only concatenate if df is not empty, otherwise use ch directly
                    df = ch if df is None or df.empty else pd.concat([df, ch], ignore_index=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} seems to not have the branch: {branch}")

        df.reset_index()

        if by == "committer" or by == "author":
            df = df.groupby(com).agg({"hours": sum})
            df = df.reset_index()
        elif by == "repository":
            df = df.groupby("repository").agg({"hours": sum})
            df = df.reset_index()

        logger.info(f"Estimated hours: {df['hours'].sum() if not df.empty else 0} total hours.")
        return df

    def commit_history(
        self,
        branch=None,
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Returns a DataFrame containing the commit history for all repositories.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to return
            days (Optional[int]): If provided, only return commits from last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with commit history
        """
        logger.info(f"Generating commit history for branch '{branch or self.default_branch}'.")
        if branch is None:
            branch = self.default_branch

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        # Initialize empty DataFrame with all required columns
        df = None

        for repo in self.repos:
            try:
                ch = repo.commit_history(
                    branch,
                    limit=limit,
                    days=days,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                if not ch.empty:
                    ch["repository"] = repo.repo_name
                    # Reset the index to make date a regular column before concatenation
                    ch = ch.reset_index()
                    df = ch if df is None else pd.concat([df, ch], sort=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} seems to not have the branch: {branch}")

        if df is None:
            # If no data was collected, return empty DataFrame with correct columns
            df = pd.DataFrame(
                columns=[
                    "repository",
                    "author",
                    "committer",
                    "date",
                    "message",
                    "commit_sha",
                    "lines",
                    "insertions",
                    "deletions",
                    "net",
                ]
            )

        # Ensure consistent column order
        df = df[
            [
                "repository",
                "author",
                "committer",
                "date",
                "message",
                "commit_sha",
                "lines",
                "insertions",
                "deletions",
                "net",
            ]
        ]
        logger.info(f"Generated commit history with {len(df)} rows.")
        return df

    def file_change_history(
        self,
        branch=None,
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Returns a DataFrame containing the file change history for all repositories.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to analyze
            days (Optional[int]): If provided, only analyze commits from last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with file change history
        """
        logger.info(f"Generating file change history for branch '{branch or self.default_branch}'.")
        if branch is None:
            branch = self.default_branch

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        # Initialize empty DataFrame with all required columns
        df = None

        for repo in self.repos:
            try:
                ch = repo.file_change_history(
                    branch,
                    limit=limit,
                    days=days,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                if not ch.empty:
                    ch = ch.copy()  # Avoid SettingWithCopyWarning
                    ch["repository"] = repo.repo_name
                    # Reset the index to make date a regular column before concatenation
                    # Use reset_index with a unique name to avoid duplicate column error
                    if "date" in ch.columns:
                        # If date column exists, use index_col=False to avoid creating a new date column
                        ch = ch.reset_index(drop=True)
                    else:
                        # Otherwise, reset the index and rename the resulting column
                        ch = ch.reset_index().rename(columns={"index": "date"})
                    df = ch if df is None else pd.concat([df, ch], ignore_index=True, sort=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} seems to not have the branch: {branch}")

        if df is None:
            # If no data was collected, return empty DataFrame with correct columns
            df = pd.DataFrame(
                columns=[
                    "repository",
                    "date",
                    "author",
                    "committer",
                    "message",
                    "filename",
                    "insertions",
                    "deletions",
                ]
            )

        # Ensure we only select columns that exist in the DataFrame
        # Start with all the columns we want
        desired_columns = [
            "repository",
            "date",
            "author",
            "committer",
            "message",
            "filename",
            "insertions",
            "deletions",
        ]
        # Filter to only include columns that exist in the DataFrame
        available_columns = [col for col in desired_columns if col in df.columns]

        # Select only available columns
        if available_columns:
            df = df[available_columns]

        logger.info(f"Generated file change history with {len(df)} rows.")
        return df

    def blame(self, committer=True, by="repository", ignore_globs=None, include_globs=None):
        """Analyzes blame information across all repositories.

        Retrieves blame information from the current HEAD of each repository and aggregates it
        based on the specified grouping. Can group results by committer/author and either
        repository or file.

        Args:
            committer (bool, optional): If True, group by committer name. If False, group by author name.
                Defaults to True.
            by (str, optional): How to group the results. One of:
                - 'repository': Group by repository (default)
                - 'file': Group by individual file
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns depending on the 'by' parameter:
                If by='repository':
                    - committer/author (str): Name of the committer/author
                    - loc (int): Lines of code attributed to that person
                If by='file':
                    - committer/author (str): Name of the committer/author
                    - file (str): File path
                    - loc (int): Lines of code attributed to that person in that file

        Note:
            Results are sorted by lines of code in descending order.
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """
        logger.info(f"Calculating blame grouped by {'committer' if committer else 'author'} and '{by}'.")
        df = None

        for repo in self.repos:
            try:
                if df is None:
                    df = repo.blame(
                        committer=committer,
                        by=by,
                        ignore_globs=ignore_globs,
                        include_globs=include_globs,
                    )
                else:
                    blame_df = repo.blame(
                        committer=committer,
                        by=by,
                        ignore_globs=ignore_globs,
                        include_globs=include_globs,
                    )
                    if not blame_df.empty:
                        df = pd.concat([df, blame_df], ignore_index=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} couldnt be blamed")
                pass

        # Reset index to convert committer/author from index to column
        df = df.reset_index()

        # Fix column naming after reset_index - the grouped column becomes 'index'
        groupby_column = "committer" if committer else "author"
        if "index" in df.columns and groupby_column not in df.columns:
            df = df.rename(columns={"index": groupby_column})
        elif groupby_column not in df.columns:
            logger.warning(
                f"Expected column '{groupby_column}' not found in blame data. Available columns: {df.columns.tolist()}"
            )
            # Return empty DataFrame with proper structure if column is missing
            return pd.DataFrame(columns=[groupby_column, "loc"])

        if committer:
            if by == "repository":
                df = df.groupby("committer")["loc"].sum().to_frame()
            elif by == "file":
                df = df.groupby(["committer", "file"])["loc"].sum().to_frame()
        else:
            if by == "repository":
                df = df.groupby("author")["loc"].sum().to_frame()
            elif by == "file":
                df = df.groupby(["author", "file"])["loc"].sum().to_frame()

        df = df.sort_values(by=["loc"], ascending=False)
        logger.info(f"Calculated blame with {len(df)} rows.")
        return df

    def file_detail(self, rev="HEAD", committer=True, ignore_globs=None, include_globs=None):
        """Provides detailed information about all files in the repositories.

        Analyzes each file in the repositories at the specified revision, gathering
        information about size, ownership, and last modification.

        Args:
            rev (str, optional): Revision to analyze. Defaults to 'HEAD'.
            committer (bool, optional): If True, use committer info. If False, use author.
                Defaults to True.
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame indexed by (file, repository) with columns:
                - committer/author (str): Name of primary committer/author
                - last_change (datetime): When file was last modified
                - loc (int): Lines of code in file
                - extension (str): File extension
                - directory (str): Directory containing file
                - filename (str): Name of file without path
                - pct_blame (float): Percentage of file attributed to primary committer/author

        Note:
            The primary committer/author is the person responsible for the most lines
            in the current version of the file.
        """
        logger.info(f"Generating file detail for revision '{rev}'.")
        df = None

        for repo in self.repos:
            try:
                if df is None:
                    df = repo.file_detail(
                        ignore_globs=ignore_globs,
                        include_globs=include_globs,
                        committer=committer,
                        rev=rev,
                    )
                    df["repository"] = repo.repo_name
                else:
                    chunk = repo.file_detail(
                        ignore_globs=ignore_globs,
                        include_globs=include_globs,
                        committer=committer,
                        rev=rev,
                    )
                    if not chunk.empty:
                        chunk = chunk.copy()  # Avoid SettingWithCopyWarning
                        chunk["repository"] = repo.repo_name
                        df = pd.concat([df, chunk], ignore_index=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} couldnt be inspected")

        df = df.reset_index()
        df = df.set_index(["file", "repository"])
        logger.info(f"Generated file detail for {len(df)} files.")
        return df

    def branches(self):
        """Returns information about all branches across repositories.

        Retrieves a list of all branches (both local and remote) from each repository
        in the project directory.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Repository name
                - local (bool): Whether the branch is local
                - branch (str): Name of the branch
        """
        logger.info("Fetching branch information for all repositories.")
        df = pd.DataFrame(columns=["repository", "local", "branch"])

        if _has_joblib:
            ds = Parallel(n_jobs=-1, backend="threading", verbose=0)(delayed(_branches_func)(x) for x in self.repos)
            for d in ds:
                if not d.empty:
                    df = pd.concat([df, d], ignore_index=True)
        else:
            for repo in self.repos:
                try:
                    branches_df = _branches_func(repo)
                    if not branches_df.empty:
                        df = pd.concat([df, branches_df], ignore_index=True)
                except GitCommandError:
                    # Use logger instead of print
                    logger.warning(f"Repo: {repo} couldn't be inspected")

        df = df.reset_index(drop=True)
        logger.info(f"Fetched branch information for {len(df)} branches.")
        return df

    def revs(self, branch=None, limit=None, skip=None, num_datapoints=None):
        """
        Returns a DataFrame containing revision information for all repositories.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of revisions to return
            skip (Optional[int]): Number of revisions to skip between samples
            num_datapoints (Optional[int]): If provided, evenly sample this many revisions

        Returns:
            DataFrame: DataFrame with revision information
        """
        logger.info(f"Fetching revisions for branch '{branch or self.default_branch}'.")
        if branch is None:
            branch = self.default_branch

        if limit is not None:
            limit = math.floor(float(limit) / len(self.repos))

        if num_datapoints is not None:
            num_datapoints = math.floor(float(num_datapoints) / len(self.repos))

        df = pd.DataFrame(columns=["repository", "rev"])

        if _has_joblib:
            ds = Parallel(n_jobs=-1, backend="threading", verbose=0)(
                [delayed(_revs_func)(repo, branch, limit, skip, num_datapoints) for repo in self.repos]
            )
            for d in ds:
                if not d.empty:
                    df = pd.concat([df, d], ignore_index=True)
        else:
            for repo in self.repos:
                try:
                    revs = repo.revs(
                        branch=branch,
                        limit=limit,
                        skip=skip,
                        num_datapoints=num_datapoints,
                    )
                    if not revs.empty:
                        revs = revs.copy()  # Avoid SettingWithCopyWarning
                        revs["repository"] = repo.repo_name
                        df = pd.concat([df, revs], ignore_index=True)
                except GitCommandError:
                    # Use logger instead of print
                    logger.warning(f"Repo: {repo} couldn't be inspected")

        df = df.reset_index(drop=True)
        logger.info(f"Fetched {len(df)} revisions.")
        return df

    def cumulative_blame(
        self,
        branch=None,
        by="committer",
        limit=None,
        skip=None,
        num_datapoints=None,
        committer=True,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Returns a DataFrame containing cumulative blame information for all repositories.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            by (str, optional): How to group results. Defaults to 'committer'.
            limit (Optional[int]): Maximum number of revisions to analyze
            skip (Optional[int]): Number of revisions to skip between samples
            num_datapoints (Optional[int]): If provided, evenly sample this many revisions
            committer (bool, optional): If True use committer, if False use author. Defaults to True.
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with cumulative blame information
        """
        logger.info(f"Calculating cumulative blame for branch '{branch or self.default_branch}' grouped by '{by}'.")
        if branch is None:
            branch = self.default_branch

        blames = []
        for repo in self.repos:
            try:
                blame = repo.cumulative_blame(
                    branch=branch,
                    limit=limit,
                    skip=skip,
                    num_datapoints=num_datapoints,
                    committer=committer,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                if not blame.empty:
                    blames.append((repo.repo_name, blame))
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} couldn't be inspected")
                pass

        if not blames:
            # Return empty DataFrame with expected columns if no data
            if by == "committer":
                return pd.DataFrame(columns=["committer"])
            elif by == "project":
                return pd.DataFrame(columns=["project"])
            else:  # by == 'raw'
                return pd.DataFrame()

        global_blame = blames[0][1]
        global_blame.columns = [x + "__" + str(blames[0][0]) for x in global_blame.columns.values]
        blames = blames[1:]
        for reponame, blame in blames:
            blame.columns = [x + "__" + reponame for x in blame.columns.values]
            global_blame = pd.merge(global_blame, blame, left_index=True, right_index=True, how="outer")

        global_blame = global_blame.ffill()
        global_blame.fillna(0.0, inplace=True)

        # Convert all numeric columns to float first
        numeric_columns = []
        for col in global_blame.columns:
            if col != "date":
                try:
                    global_blame[col] = pd.to_numeric(global_blame[col], errors="raise")
                    numeric_columns.append(col)
                except (ValueError, TypeError):
                    # Skip columns that can't be converted to numeric
                    pass

        if by == "committer":
            committers = [(str(x).split("__")[0].lower().strip(), x) for x in numeric_columns]

            if sys.version_info.major == 2:
                committer_mapping = {c: [x[1] for x in committers if x[0] == c] for c in {x[0] for x in committers}}
            else:
                committer_mapping = {c: [x[1] for x in committers if x[0] == c] for c in {x[0] for x in committers}}

            for committer in committer_mapping:
                global_blame[committer] = pd.Series(0.0, index=global_blame.index)
                for col in committer_mapping.get(committer, []):
                    global_blame[committer] += global_blame[col]

            global_blame = global_blame.reindex(columns=list(committer_mapping.keys()))
        elif by == "project":
            projects = [(str(x).split("__")[1].lower().strip(), x) for x in numeric_columns]

            if sys.version_info.major == 2:
                project_mapping = {c: [x[1] for x in projects if x[0] == c] for c in {x[0] for x in projects}}
            else:
                project_mapping = {c: [x[1] for x in projects if x[0] == c] for c in {x[0] for x in projects}}

            for project in project_mapping:
                global_blame[project] = pd.Series(0.0, index=global_blame.index)
                for col in project_mapping.get(project, []):
                    global_blame[project] += global_blame[col]

            global_blame = global_blame.reindex(columns=list(project_mapping.keys()))

        global_blame = global_blame[~global_blame.index.duplicated()]

        logger.info(f"Calculated cumulative blame with {len(global_blame)} time points.")
        return global_blame

    def commits_in_tags(self, **kwargs):
        """
        Analyze each tag, and trace backwards from the tag to all commits that make
        up that tag. This method looks at the commit for the tag, and then works
        backwards to that commits parents, and so on and so, until it hits another
        tag, is out of the time range, or hits the root commit. It returns a DataFrame
        with the branches:

        :param kwargs: kwargs to pass to ``Repository.commits_in_tags``

        :returns: DataFrame
        """
        logger.info(f"Analyzing commits in tags with kwargs: {kwargs}")
        dfs = []
        for repo in self.repos:
            try:
                dfs.append(repo.commits_in_tags(**kwargs))
            except GitCommandError as e:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} couldn't be inspected because of {e!r}")
        df = pd.concat(dfs)
        logger.info(f"Analyzed commits in tags, found {len(df)} relevant commits.")
        return df

    def tags(self):
        """
        Returns a data frame of all tags in origin.  The DataFrame will have the columns:

         * repository
         * tag

        :returns: DataFrame
        """
        logger.info("Fetching tags for all repositories.")
        if _has_joblib:
            dfs = Parallel(n_jobs=-1, backend="threading", verbose=0)(delayed(_tags_func)(x) for x in self.repos)
        else:
            dfs = []
            for repo in self.repos:
                try:
                    dfs.append(repo.tags())
                except GitCommandError:
                    # Use logger instead of print
                    logger.warning(f"Repo: {repo} couldn't be inspected")
        # Filter out empty DataFrames before concatenation
        dfs = [df for df in dfs if not df.empty]
        df = pd.concat(dfs) if dfs else pd.DataFrame()
        logger.info(f"Fetched {len(df)} tags.")
        return df

    def repo_information(self):
        """Returns detailed metadata about each repository.

        Retrieves various properties and references from each repository's
        Git object model.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - local_directory (str): Path to the repository
                - branches (list): List of branches
                - bare (bool): Whether it's a bare repository
                - remotes (list): List of remote references
                - description (str): Repository description
                - references (list): List of all references
                - heads (list): List of branch heads
                - submodules (list): List of submodules
                - tags (list): List of tags
                - active_branch (str): Currently checked out branch
        """
        logger.info("Fetching detailed repository information.")
        data = [
            [
                repo.git_dir,
                repo.repo.branches,
                repo.repo.bare,
                repo.repo.remotes,
                repo.repo.description,
                repo.repo.references,
                repo.repo.heads,
                repo.repo.submodules,
                repo.repo.tags,
                repo.repo.active_branch,
            ]
            for repo in self.repos
        ]

        df = pd.DataFrame(
            data,
            columns=[
                "local_directory",
                "branches",
                "bare",
                "remotes",
                "description",
                "references",
                "heads",
                "submodules",
                "tags",
                "active_branch",
            ],
        )

        logger.info(f"Fetched detailed information for {len(df)} repositories.")
        return df

    def bus_factor(self, ignore_globs=None, include_globs=None, by="projectd"):
        """Calculates the "bus factor" for the repositories.

        The bus factor is a measure of risk based on how concentrated the codebase knowledge is
        among contributors. It is calculated as the minimum number of contributors whose combined
        contributions account for at least 50% of the codebase's lines of code.

        Args:
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include
            by (str, optional): How to calculate the bus factor. One of:
                - 'projectd': Calculate for entire project directory (default)
                - 'repository': Calculate separately for each repository
                - 'file': Calculate separately for each file across all repositories

        Returns:
            pandas.DataFrame: A DataFrame with columns depending on the 'by' parameter:
                If by='projectd':
                    - projectd (str): Always 'projectd'
                    - bus factor (int): Bus factor for entire project
                If by='repository':
                    - repository (str): Repository name
                    - bus factor (int): Bus factor for that repository
                If by='file':
                    - file (str): File path
                    - bus factor (int): Bus factor for that file
                    - repository (str): Repository name

        Note:
            A low bus factor (e.g. 1-2) indicates high risk as knowledge is concentrated among
            few contributors. A higher bus factor indicates knowledge is better distributed.
        """
        logger.info(f"Calculating bus factor grouped by '{by}'.")
        if by == "file":
            # Calculate file-wise bus factor across all repositories
            all_file_bus_factors = []
            for repo in self.repos:
                try:
                    repo_file_bf = repo.bus_factor(ignore_globs=ignore_globs, include_globs=include_globs, by="file")
                    if not repo_file_bf.empty:
                        all_file_bus_factors.append(repo_file_bf)
                except GitCommandError:
                    logger.warning(f"Repo: {repo} couldn't be inspected for file-wise bus factor")
                    continue

            if all_file_bus_factors:
                result_df = pd.concat(all_file_bus_factors, ignore_index=True)
                logger.info(f"Calculated file-wise bus factor for {len(result_df)} files across all repositories.")
                return result_df
            else:
                logger.warning("No file-wise bus factor data could be calculated.")
                return pd.DataFrame(columns=["file", "bus factor", "repository"])
        elif by == "projectd":
            blame = self.blame(ignore_globs=ignore_globs, include_globs=include_globs, by="repository")
            blame = blame.sort_values(by=["loc"], ascending=False)

            total = blame["loc"].sum()
            cumulative = 0
            tc = 0
            for idx in range(blame.shape[0]):
                cumulative += blame.iloc[idx]["loc"]
                tc += 1
                if cumulative >= total / 2:
                    break

            logger.info(f"Calculated bus factor for the project directory: {tc}")
            return pd.DataFrame([["projectd", tc]], columns=["projectd", "bus factor"])
        elif by == "repository":
            df = pd.DataFrame(columns=["repository", "bus factor"])
            for repo in self.repos:
                try:
                    bf_df = repo.bus_factor(ignore_globs=include_globs, include_globs=include_globs, by=by)
                    if not bf_df.empty:
                        df = pd.concat([df, bf_df], ignore_index=True)
                except GitCommandError:
                    # Use logger instead of print
                    logger.warning(f"Repo: {repo} couldn't be inspected")

            df.reset_index()
            logger.info(f"Calculated bus factor for {len(df)} repositories.")
            return df

    def punchcard(
        self,
        branch=None,
        limit=None,
        days=None,
        by=None,
        normalize=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Returns a DataFrame containing punchcard data for all repositories.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to analyze
            days (Optional[int]): If provided, only analyze commits from last N days
            by (Optional[str]): How to group results. One of None, 'committer', 'author'
            normalize (Optional[int]): If provided, normalize values to this maximum
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with punchcard data
        """
        logger.info(f"Generating punchcard data for branch '{branch or self.default_branch}'.")
        if branch is None:
            branch = self.default_branch

        df = pd.DataFrame()

        repo_by = None if by == "repository" else by

        for repo in self.repos:
            try:
                chunk = repo.punchcard(
                    branch=branch,
                    limit=limit,
                    days=days,
                    by=repo_by,
                    normalize=None,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                if not chunk.empty:
                    chunk = chunk.copy()  # Avoid SettingWithCopyWarning
                    chunk["repository"] = repo.repo_name
                    df = pd.concat([df, chunk], ignore_index=True)
            except GitCommandError:
                # Use logger instead of print
                logger.warning(f"Repo: {repo} couldn't be inspected")

        df.reset_index()

        aggs = ["hour_of_day", "day_of_week"]
        if by is not None:
            aggs.append(by)

        punch_card = df.groupby(aggs).agg({"lines": np.sum, "insertions": np.sum, "deletions": np.sum, "net": np.sum})
        punch_card.reset_index(inplace=True)

        # normalize all cols
        if normalize is not None:
            for col in ["lines", "insertions", "deletions", "net"]:
                punch_card[col] = (punch_card[col] / punch_card[col].sum()) * normalize
            logger.info(f"Normalized punchcard data to max value {normalize}.")

        logger.info(f"Generated punchcard data with {len(punch_card)} entries.")
        return punch_card

    def __del__(self):
        """Cleanup method called when the object is destroyed.

        Ensures proper cleanup of all repository objects, including
        temporary directories for cloned repositories.
        """
        logger.debug("Cleaning up ProjectDirectory resources.")
        for repo in self.repos:
            try:
                repo.__del__()
            except Exception as e:
                logger.error(f"Error during cleanup of repo {repo.repo_name}: {e}")
        logger.debug("Finished cleaning up ProjectDirectory resources.")

    def _is_valid_git_repo(self, path):
        """Helper method to check if a path is a valid git repository.

        Args:
            path (str): Path to check

        Returns:
            bool: True if path is a valid git repository, False otherwise
        """
        logger.debug(f"Checking if '{path}' is a valid git repository.")
        try:
            # Check if it's a directory first
            if not os.path.isdir(path):
                return False

            # Check for .git directory (regular repository)
            git_dir = os.path.join(path, ".git")
            if os.path.exists(git_dir) and os.path.isdir(git_dir):
                return True

            # Check if it's a bare repository by looking for required files
            # In a bare repo, these files are directly in the repository root
            required_files = ["HEAD", "config", "objects", "refs"]
            is_valid = all(os.path.exists(os.path.join(path, f)) for f in required_files)
            logger.debug(f"Path '{path}' is {'valid' if is_valid else 'invalid'} git repository.")
            return is_valid
        except OSError as e:
            # Handle filesystem-related errors
            logger.error(f"OSError checking path '{path}': {e}")
            return False

    def _get_repo_name_from_path(self, path):
        """Helper method to get repository name from path.

        Args:
            path (str): Path to repository

        Returns:
            str: Repository name (last component of path)
        """
        logger.debug(f"Getting repository name from path: '{path}'")
        # For URLs, get the last part before .git
        if isinstance(path, str) and path.startswith(("git://", "https://", "http://")):
            return path.rstrip("/").split("/")[-1].replace(".git", "")
        # For local paths, use the last directory name
        name = os.path.basename(path.rstrip(os.sep))
        logger.debug(f"Determined repository name: '{name}'")
        return name

    def bulk_fetch_and_warm(
        self,
        fetch_remote=False,
        warm_cache=False,
        parallel=True,
        remote_name="origin",
        prune=False,
        dry_run=False,
        cache_methods=None,
        **kwargs,
    ):
        """Safely fetch remote changes and pre-warm cache for all repositories.

        Performs bulk operations across all repositories in the project directory,
        optionally fetching from remote repositories and pre-warming caches to
        improve subsequent analysis performance.

        Args:
            fetch_remote (bool, optional): Whether to fetch from remote repositories.
                Defaults to False.
            warm_cache (bool, optional): Whether to pre-warm repository caches.
                Defaults to False.
            parallel (bool, optional): Use parallel processing when available (joblib).
                Defaults to True.
            remote_name (str, optional): Name of remote to fetch from. Defaults to 'origin'.
            prune (bool, optional): Remove remote-tracking branches that no longer exist.
                Defaults to False.
            dry_run (bool, optional): Show what would be fetched without actually fetching.
                Defaults to False.
            cache_methods (Optional[List[str]]): List of methods to use for cache warming.
                If None, uses default methods. See Repository.warm_cache for available methods.
            **kwargs: Additional keyword arguments to pass to cache warming methods.

        Returns:
            dict: Results with keys:
                - success (bool): Whether the overall operation was successful
                - repositories_processed (int): Number of repositories processed
                - fetch_results (dict): Per-repository fetch results (if fetch_remote=True)
                - cache_results (dict): Per-repository cache warming results (if warm_cache=True)
                - execution_time (float): Total execution time in seconds
                - summary (dict): Summary statistics of the operation

        Note:
            This method safely handles errors at the repository level, ensuring that
            failures in one repository don't affect processing of others. All operations
            are read-only and will not modify working directories or current branches.
        """
        logger.info(
            f"Starting bulk operations for {len(self.repos)} repositories "
            f"(fetch_remote={fetch_remote}, warm_cache={warm_cache}, parallel={parallel})"
        )

        import time

        start_time = time.time()

        result = {
            "success": False,
            "repositories_processed": 0,
            "fetch_results": {},
            "cache_results": {},
            "execution_time": 0.0,
            "summary": {
                "fetch_successful": 0,
                "fetch_failed": 0,
                "cache_successful": 0,
                "cache_failed": 0,
                "repositories_with_remotes": 0,
                "total_cache_entries_created": 0,
            },
        }

        if not self.repos:
            result["success"] = True
            result["execution_time"] = time.time() - start_time
            logger.info("No repositories to process")
            return result

        # Define the worker function for individual repository processing
        def process_repository(repo):
            """Process a single repository for fetch and/or cache warming."""
            repo_result = {
                "repo_name": repo.repo_name,
                "fetch_result": None,
                "cache_result": None,
                "success": True,
                "error": None,
            }

            try:
                # Perform fetch if requested
                if fetch_remote:
                    logger.debug(f"Fetching remote for repository '{repo.repo_name}'")
                    fetch_result = repo.safe_fetch_remote(remote_name=remote_name, prune=prune, dry_run=dry_run)
                    repo_result["fetch_result"] = fetch_result

                    if not fetch_result["success"] and fetch_result.get("remote_exists", False):
                        # Only count as failure if remote exists but fetch failed
                        # Missing remotes are not considered failures
                        repo_result["success"] = False
                        repo_result["error"] = fetch_result.get("error", "Fetch failed")

                # Perform cache warming if requested
                if warm_cache:
                    logger.debug(f"Warming cache for repository '{repo.repo_name}'")
                    cache_result = repo.warm_cache(methods=cache_methods, **kwargs)
                    repo_result["cache_result"] = cache_result

                    if not cache_result["success"]:
                        repo_result["success"] = False
                        if repo_result["error"]:
                            repo_result["error"] += "; Cache warming failed"
                        else:
                            repo_result["error"] = "Cache warming failed"

                logger.debug(f"Completed processing repository '{repo.repo_name}' (success={repo_result['success']})")

            except Exception as e:
                repo_result["success"] = False
                repo_result["error"] = f"Unexpected error: {str(e)}"
                logger.error(f"Unexpected error processing repository '{repo.repo_name}': {e}")

            return repo_result

        # Process repositories (with or without parallel execution)
        if parallel and _has_joblib and len(self.repos) > 1:
            logger.info(f"Processing {len(self.repos)} repositories in parallel")
            try:
                from joblib import Parallel, delayed

                repo_results = Parallel(n_jobs=-1, backend="threading", verbose=0)(
                    delayed(process_repository)(repo) for repo in self.repos
                )
            except Exception as e:
                logger.warning(f"Parallel processing failed, falling back to sequential: {e}")
                repo_results = [process_repository(repo) for repo in self.repos]
        else:
            logger.info(f"Processing {len(self.repos)} repositories sequentially")
            repo_results = [process_repository(repo) for repo in self.repos]

        # Process results and build summary
        for repo_result in repo_results:
            repo_name = repo_result["repo_name"]
            result["repositories_processed"] += 1

            # Store individual results
            if fetch_remote and repo_result["fetch_result"]:
                result["fetch_results"][repo_name] = repo_result["fetch_result"]

                # Update fetch summary
                if repo_result["fetch_result"]["success"]:
                    result["summary"]["fetch_successful"] += 1
                else:
                    result["summary"]["fetch_failed"] += 1

                if repo_result["fetch_result"]["remote_exists"]:
                    result["summary"]["repositories_with_remotes"] += 1

            if warm_cache and repo_result["cache_result"]:
                result["cache_results"][repo_name] = repo_result["cache_result"]

                # Update cache summary
                if repo_result["cache_result"]["success"]:
                    result["summary"]["cache_successful"] += 1
                    result["summary"]["total_cache_entries_created"] += repo_result["cache_result"][
                        "cache_entries_created"
                    ]
                else:
                    result["summary"]["cache_failed"] += 1

        # Calculate execution time and overall success
        result["execution_time"] = time.time() - start_time

        # Consider operation successful if at least one repository was processed successfully
        successful_repos = sum(1 for repo_result in repo_results if repo_result["success"])
        result["success"] = successful_repos > 0

        # Log summary
        if result["success"]:
            logger.info(
                f"Bulk operations completed successfully in {result['execution_time']:.2f} seconds. "
                f"Processed {result['repositories_processed']} repositories, "
                f"{successful_repos} successful, {len(repo_results) - successful_repos} failed."
            )

            if fetch_remote:
                logger.info(
                    f"Fetch summary: {result['summary']['fetch_successful']} successful, "
                    f"{result['summary']['fetch_failed']} failed, "
                    f"{result['summary']['repositories_with_remotes']} have remotes"
                )

            if warm_cache:
                logger.info(
                    f"Cache warming summary: {result['summary']['cache_successful']} successful, "
                    f"{result['summary']['cache_failed']} failed, "
                    f"{result['summary']['total_cache_entries_created']} total cache entries created"
                )
        else:
            logger.warning(
                f"Bulk operations completed with errors in {result['execution_time']:.2f} seconds. "
                f"No repositories processed successfully."
            )

        return result

    def invalidate_cache(self, keys=None, pattern=None, repositories=None):
        """Invalidate cache entries across multiple repositories.

        Args:
            keys (Optional[List[str]]): List of specific cache keys to invalidate
            pattern (Optional[str]): Pattern to match cache keys (supports * wildcard)
            repositories (Optional[List[str]]): List of repository names to target.
                If None, all repositories are targeted.

        Returns:
            dict: Results with total invalidated and per-repository breakdown
        """
        result = {"total_invalidated": 0, "repositories_processed": 0, "repository_results": {}}

        target_repos = self.repos
        if repositories:
            target_repos = [repo for repo in self.repos if repo.repo_name in repositories]

        for repo in target_repos:
            result["repositories_processed"] += 1
            try:
                count = repo.invalidate_cache(keys=keys, pattern=pattern)
                result["repository_results"][repo.repo_name] = {"success": True, "invalidated": count}
                result["total_invalidated"] += count
            except Exception as e:
                logger.error(f"Error invalidating cache for repository '{repo.repo_name}': {e}")
                result["repository_results"][repo.repo_name] = {"success": False, "error": str(e), "invalidated": 0}

        logger.info(
            f"Cache invalidation completed. Total invalidated: {result['total_invalidated']} "
            f"across {result['repositories_processed']} repositories"
        )

        return result

    def get_cache_stats(self):
        """Get comprehensive cache statistics across all repositories.

        Returns:
            dict: Aggregated cache statistics and per-repository breakdown
        """
        result = {
            "project_directory": str(self.git_dir) if hasattr(self, "git_dir") else "N/A",
            "total_repositories": len(self.repos),
            "repositories_with_cache": 0,
            "total_cache_entries": 0,
            "cache_backends": {},
            "global_stats": None,
            "repository_stats": {},
        }

        # Get stats from first repository with cache backend for global stats
        for repo in self.repos:
            if repo.cache_backend is not None:
                try:
                    stats = repo.get_cache_stats()
                    result["global_stats"] = stats.get("global_cache_stats")
                    break
                except Exception:
                    continue

        # Collect stats from all repositories
        for repo in self.repos:
            repo_stats = repo.get_cache_stats()
            result["repository_stats"][repo.repo_name] = repo_stats

            if repo_stats["cache_backend"] is not None:
                result["repositories_with_cache"] += 1
                result["total_cache_entries"] += repo_stats["repository_entries"]

                # Count cache backend types
                backend_type = repo_stats["cache_backend"]
                result["cache_backends"][backend_type] = result["cache_backends"].get(backend_type, 0) + 1

        # Add summary percentages
        if result["total_repositories"] > 0:
            result["cache_coverage_percent"] = (result["repositories_with_cache"] / result["total_repositories"]) * 100
        else:
            result["cache_coverage_percent"] = 0.0

        logger.info(
            f"Cache statistics collected for {result['total_repositories']} repositories. "
            f"Cache coverage: {result['cache_coverage_percent']:.1f}%, "
            f"Total entries: {result['total_cache_entries']}"
        )

        return result


class GitHubProfile(ProjectDirectory):
    """A specialized ProjectDirectory for analyzing a GitHub user's repositories.

    This class extends ProjectDirectory to work with a GitHub user's public profile,
    automatically discovering and analyzing their repositories.

    Args:
        username (str): GitHub username to analyze
        ignore_forks (bool, optional): Whether to exclude forked repositories.
            Defaults to False.
        ignore_repos (Optional[List[str]]): List of repository names to ignore
        verbose (bool, optional): Whether to print verbose output. Defaults to False.

    Note:
        This class uses the GitHub API to discover repositories. It does not require
        authentication for public repositories, but API rate limits may apply.
    """

    def __init__(self, username, ignore_forks=False, ignore_repos=None, verbose=False):
        """Initializes a GitHubProfile object.

        Args:
            username (str): GitHub username to analyze
            ignore_forks (bool, optional): Whether to exclude forked repositories.
                Defaults to False.
            ignore_repos (Optional[List[str]]): List of repository names to ignore
            verbose (bool, optional): Whether to print verbose output. Defaults to False.
        """
        logger.info(f"Initializing GitHubProfile for user '{username}'.")
        # pull the git urls from github's api
        uri = f"https://api.github.com/users/{username}/repos"
        logger.debug(f"Fetching repositories from GitHub API: {uri}")
        try:
            data = requests.get(uri)
            data.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            json_data = data.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch GitHub repositories for user {username}: {e}")
            # Initialize with empty list if API call fails
            ProjectDirectory.__init__(self, working_dir=[], ignore_repos=ignore_repos, verbose=verbose)
            return

        repos = []
        for chunk in json_data:
            # if we are skipping forks
            if ignore_forks:
                if not chunk["fork"]:
                    repos.append(chunk["git_url"])
            else:
                repos.append(chunk["git_url"])

        logger.info(f"Found {len(repos)} repositories for user '{username}' (ignore_forks={ignore_forks}).")
        ProjectDirectory.__init__(self, working_dir=repos, ignore_repos=ignore_repos, verbose=verbose)
