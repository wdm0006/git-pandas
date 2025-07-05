"""
.. module:: repository
   :platform: Unix, Windows
   :synopsis: A module for examining a single git repository

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

import fnmatch
import json
import logging
import os
import shutil
import sys
import tempfile
import time

import git  # Import the full git module
import numpy as np
import pandas as pd
from git import BadName, BadObject, GitCommandError, Repo
from pandas import DataFrame, to_datetime

from gitpandas.cache import multicache
from gitpandas.logging import logger

try:
    from joblib import Parallel, delayed

    _has_joblib = True
except ImportError:
    _has_joblib = False

__author__ = "willmcginnis"


def _parallel_cumulative_blame_func(self_, x, committer, ignore_globs, include_globs):
    blm = self_.blame(
        rev=x["rev"],
        committer=committer,
        ignore_globs=ignore_globs,
        include_globs=include_globs,
    )
    blame_data = json.loads(blm.to_json())
    if "loc" in blame_data:
        x.update(blame_data["loc"])
    else:
        # If no blame data, ensure we have at least one committer with 0 lines
        x["Test User"] = 0

    return x


class Repository:
    """A class for analyzing a single git repository.

    This class provides functionality to analyze a git repository, whether it is a local
    repository or a remote repository that needs to be cloned. It offers methods for
    analyzing commit history, blame information, file changes, and other git metrics.

    Args:
        working_dir (Optional[str]): Path to the git repository:
            - If None: Uses current working directory
            - If local path: Path must contain a .git directory
            - If git URL: Repository will be cloned to a temporary directory
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
        tmp_dir (Optional[str]): Directory to clone remote repositories into. Created if not provided.
        cache_backend (Optional[object]): Cache backend instance from gitpandas.cache
        labels_to_add (Optional[List[str]]): Extra labels to add to output DataFrames
        default_branch (Optional[str]): Name of the default branch to use. If None, will try to detect
            'main' or 'master', and if neither exists, will raise ValueError.

    Attributes:
        verbose (bool): Whether verbose output is enabled
        git_dir (str): Path to the git repository
        repo (git.Repo): GitPython Repo instance
        cache_backend (Optional[object]): Cache backend being used
        _labels_to_add (List[str]): Labels to add to DataFrames
        _git_repo_name (Optional[str]): Repository name for remote repos
        default_branch (str): Name of the default branch

    Raises:
        ValueError: If default_branch is None and neither 'main' nor 'master' branch exists

    Examples:
        >>> # Create from local repository
        >>> repo = Repository('/path/to/repo')

        >>> # Create from remote repository
        >>> repo = Repository('git://github.com/user/repo.git')

    Note:
        When using remote repositories, they will be cloned to temporary directories.
        This can be slow for large repositories.
    """

    def __init__(
        self,
        working_dir=None,
        verbose=False,
        tmp_dir=None,
        cache_backend=None,
        labels_to_add=None,
        default_branch=None,
    ):
        """Initialize a Repository instance.

        Args:
            working_dir (Optional[str]): Path to the git repository:
                - If None: Uses current working directory
                - If local path: Path must contain a .git directory
                - If git URL: Repository will be cloned to a temporary directory
            verbose (bool, optional): Whether to print verbose output. Defaults to False.
            tmp_dir (Optional[str]): Directory to clone remote repositories into. Created if not provided.
            cache_backend (Optional[object]): Cache backend instance from gitpandas.cache
            labels_to_add (Optional[List[str]]): Extra labels to add to output DataFrames
            default_branch (Optional[str]): Name of the default branch to use. If None, will try to detect
                'main' or 'master', and if neither exists, will raise ValueError.

        Raises:
            ValueError: If default_branch is None and neither 'main' nor 'master' branch exists
        """
        self.verbose = verbose
        self.__delete_hook = False
        self._git_repo_name = None
        self.cache_backend = cache_backend
        self._labels_to_add = labels_to_add or []

        # Convert PosixPath to string if needed
        if working_dir is not None:
            working_dir = str(working_dir)

        if working_dir is not None:
            if working_dir.startswith(("git://", "https://", "http://")):
                # if a tmp dir is passed, clone into that, otherwise make a temp directory.
                if tmp_dir is None:
                    if self.verbose:
                        print(f"cloning repository: {working_dir} into a temporary location")
                    dir_path = tempfile.mkdtemp()
                else:
                    dir_path = tmp_dir

                logger.info(f"Cloning remote repository {working_dir} to {dir_path}")
                self.repo = Repo.clone_from(working_dir, dir_path)
                self._git_repo_name = working_dir.split(os.sep)[-1].split(".")[0]
                self.git_dir = dir_path
                self.__delete_hook = True
            else:
                self.git_dir = working_dir
                self.repo = Repo(self.git_dir)
        else:
            self.git_dir = os.getcwd()
            self.repo = Repo(self.git_dir)

        # Smart default branch detection
        if default_branch is None:
            if self.has_branch("main"):
                self.default_branch = "main"
            elif self.has_branch("master"):
                self.default_branch = "master"
            else:
                raise ValueError(
                    "Could not detect default branch. Neither 'main' nor 'master' exists. "
                    "Please specify default_branch explicitly."
                )
        else:
            self.default_branch = default_branch

        if self.verbose:
            print(
                f"Repository [{self._repo_name()}] instantiated at directory: {self.git_dir} "
                f"with default branch: {self.default_branch}"
            )
        logger.info(
            f"Repository [{self._repo_name()}] instantiated at directory: {self.git_dir} "
            f"with default branch: {self.default_branch}"
        )

    def __del__(self):
        """Cleanup method called when the object is destroyed.

        Cleans up any temporary directories created for cloned repositories.
        """
        if self.__delete_hook and os.path.exists(self.git_dir):
            shutil.rmtree(self.git_dir)

    @multicache(key_prefix="is_bare", key_list=[])
    def is_bare(self):
        """Checks if this is a bare repository.

        A bare repository is one without a working tree, typically used as a central
        repository.

        Returns:
            bool: True if this is a bare repository, False otherwise
        """

        return self.repo.bare

    @multicache(key_prefix="has_coverage", key_list=[])
    def has_coverage(self):
        """Checks if a parseable .coverage file exists in the repository.

        Attempts to find and parse a .coverage file in the repository root directory.
        The file must be in a valid format that can be parsed as JSON.

        Returns:
            bool: True if a valid .coverage file exists, False otherwise
        """

        return os.path.exists(self.git_dir + os.sep + ".coverage")

    @multicache(key_prefix="coverage", key_list=[])
    def coverage(self):
        """Analyzes test coverage information from the repository.

        Attempts to read and parse the .coverage file in the repository root
        using the coverage.py API. Returns coverage statistics for each file.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - filename (str): Path to the file
                - lines_covered (int): Number of lines covered by tests
                - total_lines (int): Total number of lines
                - coverage (float): Coverage percentage
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            Returns an empty DataFrame if no coverage data exists or can't be read.
        """
        if not self.has_coverage():
            return DataFrame(columns=["filename", "lines_covered", "total_lines", "coverage"])

        try:
            import coverage

            cov = coverage.Coverage(data_file=os.path.join(self.git_dir, ".coverage"))
            cov.load()
            data = cov.get_data()

            ds = []
            for filename in data.measured_files():
                try:
                    with open(os.path.join(self.git_dir, filename)) as f:
                        total_lines = sum(1 for _ in f)
                    lines_covered = len(data.lines(filename) or [])
                    short_filename = filename.replace(self.git_dir + os.sep, "")
                    ds.append([short_filename, lines_covered, total_lines])
                except OSError as e:
                    logger.warning(f"Could not process coverage for file {filename}: {e}")

            if not ds:
                return DataFrame(columns=["filename", "lines_covered", "total_lines", "coverage"])

            df = DataFrame(ds, columns=["filename", "lines_covered", "total_lines"])
            df["coverage"] = df["lines_covered"] / df["total_lines"]
            df = self._add_labels_to_df(df)

            return df

        except FileNotFoundError as e:
            logger.warning(f"Coverage file not found: {e}")
            return DataFrame(columns=["filename", "lines_covered", "total_lines", "coverage"])
        except PermissionError as e:
            logger.error(f"Permission denied accessing coverage file: {e}")
            return DataFrame(columns=["filename", "lines_covered", "total_lines", "coverage"])
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid coverage data format: {e}")
            return DataFrame(columns=["filename", "lines_covered", "total_lines", "coverage"])
        except Exception as e:
            logger.error(f"Unexpected error analyzing coverage data: {e}", exc_info=True)
            return DataFrame(columns=["filename", "lines_covered", "total_lines", "coverage"])

    @multicache(
        key_prefix="hours_estimate",
        key_list=[
            "branch",
            "grouping_window",
            "single_commit_hours",
            "limit",
            "days",
            "committer",
            "ignore_globs",
            "include_globs",
        ],
    )
    def hours_estimate(
        self,
        branch=None,
        grouping_window=0.5,
        single_commit_hours=0.5,
        limit=None,
        days=None,
        committer=True,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        inspired by: https://github.com/kimmobrunfeldt/git-hours/blob/8aaeee237cb9d9028e7a2592a25ad8468b1f45e4/index.js#L114-L143

        Iterates through the commit history of repo to estimate the time commitement of each author or committer over
        the course of time indicated by limit/extensions/days/etc.

        :param branch: (optional, default=None) the branch to return commits for, defaults to default_branch if None
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param grouping_window: (optional, default=0.5 hours) the threhold for how close two commits need to be to
             consider them part of one coding session
        :param single_commit_hours: (optional, default 0.5 hours) the time range to associate with one single commit
        :param days: (optional, default=None) number of days to return, if limit is None
        :param committer: (optional, default=True) whether to use committer vs. author
        :param ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
        :param include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.
        :return: DataFrame
        """
        if branch is None:
            branch = self.default_branch

        logger.info(f"Starting hours estimation for branch '{branch}'")

        max_diff_in_minutes = grouping_window * 60.0
        first_commit_addition_in_minutes = single_commit_hours * 60.0

        # First get the commit history
        ch = self.commit_history(
            branch=branch,
            limit=limit,
            days=days,
            ignore_globs=ignore_globs,
            include_globs=include_globs,
        )

        # split by committer|author
        by = "committer" if committer else "author"
        people = set(ch[by].values)

        ds = []
        for person in people:
            commits = ch[ch[by] == person]
            commits_ts = [x * 10e-10 for x in sorted(commits.index.values.tolist())]

            if len(commits_ts) < 2:
                ds.append([person, 0])
                continue

            def estimate(index, date, commits_ts):
                next_ts = commits_ts[index + 1]
                diff_in_minutes = next_ts - date
                diff_in_minutes /= 60.0
                if diff_in_minutes < max_diff_in_minutes:
                    return diff_in_minutes / 60.0
                return first_commit_addition_in_minutes / 60.0

            hours = [estimate(a, b, commits_ts) for a, b in enumerate(commits_ts[:-1])]
            hours = sum(hours)
            ds.append([person, hours])

        df = DataFrame(ds, columns=[by, "hours"])
        df = self._add_labels_to_df(df)

        logger.info(f"Finished hours estimation for branch '{branch}'. Found data for {len(df)} contributors.")
        return df

    @multicache(key_prefix="commit_history", key_list=["branch", "limit", "days", "ignore_globs", "include_globs"])
    def commit_history(
        self,
        branch=None,
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """
        Returns a DataFrame containing the commit history for a branch.

        Retrieves the commit history for the specified branch, with options to limit
        the number of commits or time range, and filter which files to include.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to return
            days (Optional[int]): If provided, only return commits from the last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: A DataFrame with columns:
                - date (datetime, index): Timestamp of the commit
                - author (str): Name of the commit author
                - committer (str): Name of the committer
                - message (str): Commit message
                - commit_sha (str): Commit hash
                - lines (int): Total lines changed
                - insertions (int): Lines added
                - deletions (int): Lines removed
                - net (int): Net lines changed (insertions - deletions)
                - repository (str): Repository name

        Note:
            If both ignore_globs and include_globs are provided, files must match an include
            pattern and not match any ignore patterns to be included.
        """
        if branch is None:
            branch = self.default_branch

        logger.info(f"Fetching commit history for branch '{branch}'. Limit: {limit}, Days: {days}")

        # setup the data-set of commits
        commit_count = 0
        if limit is None:
            if days is None:
                ds = [
                    [
                        x.author.name,
                        x.committer.name,
                        x.committed_date,
                        x.message,
                        x.hexsha,
                        self.__check_extension(
                            x.stats.files,
                            ignore_globs=ignore_globs,
                            include_globs=include_globs,
                        ),
                    ]
                    for x in self.repo.iter_commits(branch)
                ]
            else:
                ds = []
                c_date = time.time()
                commits = self.repo.iter_commits(branch)
                dlim = time.time() - days * 24 * 3600
                while c_date > dlim:
                    try:
                        if sys.version_info.major == 2:
                            x = commits.next()
                        else:
                            x = commits.__next__()
                    except StopIteration:
                        break
                    c_date = x.committed_date
                    if c_date > dlim:
                        commit_count += 1
                        if logger.isEnabledFor(logging.DEBUG) and commit_count % 1000 == 0:
                            logger.debug(f"Processed {commit_count} commits (days filter)...")
                        ds.append(
                            [
                                x.author.name,
                                x.committer.name,
                                x.committed_date,
                                x.message,
                                x.hexsha,
                                self.__check_extension(
                                    x.stats.files,
                                    ignore_globs=ignore_globs,
                                    include_globs=include_globs,
                                ),
                            ]
                        )

        else:
            ds = [
                [
                    x.author.name,
                    x.committer.name,
                    x.committed_date,
                    x.message,
                    x.hexsha,
                    self.__check_extension(
                        x.stats.files,
                        ignore_globs=ignore_globs,
                        include_globs=include_globs,
                    ),
                ]
                for x in self.repo.iter_commits(branch, max_count=limit)
            ]
            commit_count = len(ds)  # Count is known due to max_count
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Processed {commit_count} commits (limit applied).")

        # aggregate stats
        ds = [
            x[:-1]
            + [
                sum([x[-1][key]["lines"] for key in x[-1]]),
                sum([x[-1][key]["insertions"] for key in x[-1]]),
                sum([x[-1][key]["deletions"] for key in x[-1]]),
                sum([x[-1][key]["insertions"] for key in x[-1]]) - sum([x[-1][key]["deletions"] for key in x[-1]]),
            ]
            for x in ds
            if len(x[-1].keys()) > 0
        ]

        # make it a pandas dataframe
        df = DataFrame(
            ds,
            columns=[
                "author",
                "committer",
                "date",
                "message",
                "commit_sha",
                "lines",
                "insertions",
                "deletions",
                "net",
            ],
        )

        # format the date col and make it the index
        df["date"] = pd.to_datetime(df["date"], unit="s", utc=True)
        df = df.set_index("date")

        df["branch"] = branch
        df = self._add_labels_to_df(df)

        logger.info(f"Finished fetching commit history for branch '{branch}'. Found {len(df)} relevant commits.")
        return df

    @multicache(key_prefix="file_change_history", key_list=["branch", "limit", "days", "ignore_globs", "include_globs"])
    def file_change_history(
        self,
        branch=None,
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None,
        skip_broken=True,
    ):
        """Returns data on commit history of files.

        For each file changed in each commit within the given parameters, returns
        information about insertions, deletions, and commit metadata.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to return, None for no limit
            days (Optional[int]): Number of days to return if limit is None
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include
            skip_broken (bool, optional): Whether to skip corrupted Git objects. Defaults to True.

        Returns:
            pandas.DataFrame: A DataFrame indexed by commit timestamp containing file change data.
                Columns include:
                - filename (str): Path to the file
                - insertions (int): Number of lines inserted
                - deletions (int): Number of lines deleted
                - lines (int): Current line count (insertions - deletions)
                - message (str): Commit message
                - committer (str): Name of the committer
                - author (str): Name of the author
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            Files matching both include_globs and ignore_globs patterns will be excluded.
        """
        if branch is None:
            branch = self.default_branch

        logger.info(
            f"Fetching file change history for branch '{branch}'. Limit: {limit}, Days: {days}, "
            f"Ignore: {ignore_globs}, Include: {include_globs}, Skip Broken: {skip_broken}"
        )

        history = []
        if limit is None and days is not None:
            try:
                cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
                for x in self.repo.iter_commits(branch):
                    if pd.to_datetime(x.committed_date, unit="s", utc=True) < cutoff:
                        break
                    try:
                        # Access commit properties safely to avoid common Git errors
                        self._process_commit_for_file_history(x, history, ignore_globs, include_globs, skip_broken)
                    except (git.exc.GitCommandError, ValueError) as e:
                        if skip_broken:
                            logger.warning(f"Skipping commit {x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}")
                            continue
                        else:
                            logger.error(
                                f"Error processing commit {x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}"
                            )
                            raise
                    except Exception as e:
                        if skip_broken:
                            logger.warning(
                                f"Unexpected error processing commit "
                                f"{x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}"
                            )
                            continue
                        else:
                            logger.error(
                                f"Unexpected error processing commit "
                                f"{x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}"
                            )
                            raise
            except git.exc.GitCommandError as e:
                logger.error(f"Error listing commits for branch '{branch}': {e}")
                return pd.DataFrame(
                    columns=["filename", "insertions", "deletions", "lines", "message", "committer", "author"]
                )
        else:
            try:
                for i, x in enumerate(self.repo.iter_commits(branch)):
                    if limit is not None and i >= limit:
                        break
                    try:
                        # Access commit properties safely to avoid common Git errors
                        self._process_commit_for_file_history(x, history, ignore_globs, include_globs, skip_broken)
                    except (git.exc.GitCommandError, ValueError) as e:
                        if skip_broken:
                            logger.warning(f"Skipping commit {x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}")
                            continue
                        else:
                            logger.error(
                                f"Error processing commit {x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}"
                            )
                            raise
                    except Exception as e:
                        if skip_broken:
                            logger.warning(
                                f"Unexpected error processing commit "
                                f"{x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}"
                            )
                            continue
                        else:
                            logger.error(
                                f"Unexpected error processing commit "
                                f"{x.hexsha if hasattr(x, 'hexsha') else 'unknown'}: {e}"
                            )
                            raise
            except git.exc.GitCommandError as e:
                logger.error(f"Error listing commits for branch '{branch}': {e}")
                return pd.DataFrame(
                    columns=["filename", "insertions", "deletions", "lines", "message", "committer", "author"]
                )

        # Return empty DataFrame with correct columns if no valid commits found
        if not history:
            logger.warning(f"No valid file change history found for branch '{branch}'")
            df = pd.DataFrame(
                columns=["filename", "insertions", "deletions", "lines", "message", "committer", "author"]
            )
            df = self._add_labels_to_df(df)
            return df

        # Create DataFrame from the collected history data
        df = pd.DataFrame(history)
        df = df.reset_index(drop=True)

        # Convert date column to datetime and set as index
        df["date"] = pd.to_datetime(df["date"], unit="s", utc=True)
        df = df.set_index(keys=["date"], drop=False)
        df = df.sort_index()

        # Add repository labels
        df = self._add_labels_to_df(df)

        logger.info(f"Finished fetching file change history for branch '{branch}'. Found {len(df)} file changes.")
        return df

    def _process_commit_for_file_history(self, commit, history, ignore_globs, include_globs, skip_broken):
        """Helper method to process a commit for file change history.

        Args:
            commit: The commit object to process
            history: List to append the file change data to
            ignore_globs: List of glob patterns for files to ignore
            include_globs: List of glob patterns for files to include
            skip_broken: Whether to skip errors for specific files
        """
        # Get commit metadata safely
        try:
            c_date = commit.committed_date
            c_message = commit.message
            c_author = commit.author.name if hasattr(commit.author, "name") else "Unknown"
            c_committer = commit.committer.name if hasattr(commit.committer, "name") else "Unknown"
            hexsha = commit.hexsha
        except (ValueError, AttributeError) as e:
            if skip_broken:
                logger.warning(f"Error accessing commit metadata: {e}")
                return
            else:
                raise

        # Get parent
        parent = commit.parents[0] if commit.parents else None

        # Process each file in the commit
        try:
            diffs = commit.diff(parent) if parent else commit.diff(git.NULL_TREE)

            for diff in diffs:
                try:
                    # Get file path
                    if diff.a_path:
                        path = diff.a_path
                    elif diff.b_path:
                        path = diff.b_path
                    else:
                        logger.warning(f"Skipping diff with no path in commit {hexsha}")
                        continue

                    # Apply glob filtering - skip filtered files
                    if not self.__check_extension({path: path}, ignore_globs, include_globs):
                        continue

                    # Extract the stats
                    insertions = 0
                    deletions = 0
                    try:
                        # Check if diff has stats attribute first
                        if hasattr(diff, "stats"):
                            stats = diff.stats
                            insertions = stats.get("insertions", 0)
                            deletions = stats.get("deletions", 0)
                        else:
                            # Alternative approach for newer GitPython versions where stats may not be available
                            # Calculate insertions and deletions manually from the diff
                            diff_content = diff.diff
                            # Check if diff.diff is bytes or string
                            if isinstance(diff_content, bytes):
                                diff_lines = diff_content.decode("utf-8", errors="replace").splitlines()
                            elif isinstance(diff_content, str):
                                diff_lines = diff_content.splitlines()
                            else:
                                # If it's neither bytes nor string, we can't process it
                                logger.warning(f"Diff content has unexpected type: {type(diff_content)}")
                                continue

                            for line in diff_lines:
                                if line.startswith("+") and not line.startswith("+++"):
                                    insertions += 1
                                elif line.startswith("-") and not line.startswith("---"):
                                    deletions += 1
                    except (ValueError, AttributeError, KeyError, UnicodeDecodeError) as e:
                        if skip_broken:
                            logger.warning(f"Error getting diff stats for {path} in commit {hexsha}: {e}")
                            continue
                        else:
                            raise

                    # Add to history
                    history.append(
                        {
                            "filename": path,
                            "insertions": insertions,
                            "deletions": deletions,
                            "lines": insertions - deletions,
                            "message": c_message,
                            "committer": c_committer,
                            "author": c_author,
                            "date": c_date,
                        }
                    )
                except Exception as e:
                    if skip_broken:
                        logger.warning(f"Error processing diff in commit {hexsha}: {e}")
                        continue
                    else:
                        raise
        except git.exc.GitCommandError as e:
            if skip_broken:
                logger.warning(f"Git error getting diffs for commit {hexsha}: {e}")
                return
            else:
                raise
        except Exception as e:
            if skip_broken:
                logger.warning(f"Unexpected error processing commit {hexsha}: {e}")
                return
            else:
                raise

    @multicache(
        key_prefix="file_change_rates",
        key_list=["branch", "limit", "coverage", "days", "ignore_globs", "include_globs"],
    )
    def file_change_rates(
        self,
        branch=None,
        limit=None,
        coverage=False,
        days=None,
        ignore_globs=None,
        include_globs=None,
        skip_broken=True,
    ):
        """
        Returns a DataFrame with file change rates, calculated as the number of changes
        between the first commit for that file and the last. If coverage is true, it will
        also calculate test coverage statistics for python source files.

        Args:
            branch (Optional[str]): Which branch to analyze. If None, uses default_branch.
            limit (Optional[int]): How many commits to go back in history. None for all.
            coverage (bool): Whether to calculate test coverage stats. Defaults to False.
            days (Optional[int]): If not None, only consider changes in the last x days.
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include
            skip_broken (bool, optional): Whether to skip corrupted Git objects. Defaults to True.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - file (str): Path to the file
                - unique_committers (int): Number of unique committers
                - abs_rate_of_change (float): Absolute rate of change
                - net_rate_of_change (float): Net rate of change
                - net_change (int): Net lines changed
                - abs_change (int): Absolute lines changed
                - edit_rate (float): Edit rate
                - lines (int): Current line count
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add
        """
        if branch is None:
            branch = self.default_branch

        logger.info(
            f"Calculating file change rates for branch '{branch}'. "
            f"Limit: {limit}, Coverage: {coverage}, Days: {days}, "
            f"Ignore: {ignore_globs}, Include: {include_globs}"
        )

        try:
            # Get file change history, passing skip_broken parameter
            fch = self.file_change_history(
                branch=branch,
                limit=limit,
                days=days,
                ignore_globs=ignore_globs,
                include_globs=include_globs,
                skip_broken=skip_broken,
            )

            # If file_change_history returns empty DataFrame, return empty DataFrame
            if fch.empty:
                logger.warning(f"No file change history data found for '{branch}'. Returning empty DataFrame.")
                return pd.DataFrame(
                    columns=[
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
                )

            # Reset index if not already done to make date a column
            if isinstance(fch.index, pd.DatetimeIndex) and "date" not in fch.columns:
                fch = fch.reset_index()

            # Group by filename and compute detailed stats
            if fch.shape[0] > 0:
                file_history = fch.groupby("filename").agg(
                    {
                        "insertions": ["sum", "max", "mean"],
                        "deletions": ["sum", "max", "mean"],
                        "message": lambda x: " | ".join([str(y) for y in x]),
                        "committer": lambda x: " | ".join([str(y) for y in x]),
                        "author": lambda x: " | ".join([str(y) for y in x]),
                        "date": ["max", "min"],
                    }
                )

                # Flatten column names
                file_history.columns = [
                    "total_insertions",
                    "insertions_max",
                    "mean_insertions",
                    "total_deletions",
                    "deletions_max",
                    "mean_deletions",
                    "messages",
                    "committers",
                    "authors",
                    "max_date",
                    "min_date",
                ]

                # Reset index to make filename a column
                file_history = file_history.reset_index()

                # Rename filename to file for consistency
                file_history = file_history.rename(columns={"filename": "file"})

                # Calculate net changes
                file_history["net_change"] = file_history["total_insertions"] - file_history["total_deletions"]
                file_history["abs_change"] = file_history["total_insertions"] + file_history["total_deletions"]

                # Calculate time deltas - ensure it's at least 1 day to avoid division by zero
                file_history["delta_time"] = file_history["max_date"] - file_history["min_date"]
                file_history["delta_days"] = file_history["delta_time"].dt.total_seconds() / (60 * 60 * 24)
                file_history["delta_days"] = file_history["delta_days"].apply(lambda x: max(1.0, x))

                # Calculate metrics
                file_history["net_rate_of_change"] = file_history["net_change"] / file_history["delta_days"]
                file_history["abs_rate_of_change"] = file_history["abs_change"] / file_history["delta_days"]
                file_history["edit_rate"] = file_history["abs_rate_of_change"] - file_history["net_rate_of_change"]
                file_history["unique_committers"] = file_history["committers"].apply(lambda x: len(set(x.split(" | "))))
                file_history["lines"] = file_history["net_change"]  # For compatibility with simplified version

                # Select key columns for the output
                rates = file_history[
                    [
                        "file",
                        "unique_committers",
                        "abs_rate_of_change",
                        "net_rate_of_change",
                        "net_change",
                        "abs_change",
                        "edit_rate",
                        "lines",
                    ]
                ]

                # Sort by edit rate
                rates = rates.sort_values("edit_rate", ascending=False)

                # Add coverage data if requested
                if coverage:
                    cov = self.coverage()
                    if not cov.empty:
                        # Ensure coverage DataFrame has 'file' as column, not index
                        if "file" not in cov.columns and "filename" in cov.columns:
                            cov = cov.rename(columns={"filename": "file"})
                        elif "file" not in cov.columns and isinstance(cov.index.name, str) and cov.index.name == "file":
                            cov = cov.reset_index()
                        rates = pd.merge(rates, cov, on="file", how="left")

                # Add repository name
                rates = self._add_labels_to_df(rates)

                return rates
            else:
                # If no file history after grouping, return empty DataFrame
                logger.warning(f"No valid file change data could be analyzed for '{branch}'.")
                return pd.DataFrame(
                    columns=[
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
                )

        except MemoryError as e:
            logger.error(f"Out of memory calculating file change rates. Try reducing limit or using days filter: {e}")
            return pd.DataFrame(
                columns=[
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
            )
        except git.exc.GitCommandError as e:
            logger.error(f"Git command failed while calculating file change rates: {e}")
            return pd.DataFrame(
                columns=[
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
            )
        except Exception as e:
            logger.error(f"Unexpected error calculating file change rates: {e}", exc_info=True)
            return pd.DataFrame(
                columns=[
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
            )

    @staticmethod
    def __check_extension(files, ignore_globs=None, include_globs=None):
        """
        Internal method to filter a list of file changes by extension and ignore_dirs.

        :param files:
        :param ignore_globs: a list of globs to ignore (if none falls back to extensions and ignore_dir)
        :param include_globs: a list of globs to include (if none, includes all).
        :return: dict
        """
        logger.debug(
            f"Checking extensions/globs. Files: {len(files)}, Ignore: {ignore_globs}, Include: {include_globs}"
        )

        if include_globs is None or include_globs == []:
            include_globs = ["*"]

        out = {}
        for key in files:
            # count up the number of patterns in the ignore globs list that match
            if ignore_globs is not None:
                count_exclude = sum([1 if fnmatch.fnmatch(key, g) else 0 for g in ignore_globs])
            else:
                count_exclude = 0

            # count up the number of patterns in the include globs list that match
            count_include = sum([1 if fnmatch.fnmatch(key, g) else 0 for g in include_globs])

            # if we have one vote or more to include and none to exclude, then we use the file.
            if count_include > 0 and count_exclude == 0:
                out[key] = files[key]

        logger.debug(f"Finished checking extensions. Filtered files count: {len(out)}")
        return out

    @multicache(key_prefix="blame", key_list=["rev", "committer", "by", "ignore_blobs", "include_globs"])
    def blame(
        self,
        rev="HEAD",
        committer=True,
        by="repository",
        ignore_globs=None,
        include_globs=None,
    ):
        """Analyzes blame information for files in the repository.

        Retrieves blame information from a specific revision and aggregates it based on
        the specified grouping. Can group results by committer/author and either
        repository or file.

        Args:
            rev (str, optional): Revision to analyze. Defaults to 'HEAD'.
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
        logger.info(f"Calculating blame for rev '{rev}'. Group by: {by}, Committer: {committer}")
        logger.debug(f"Blame Ignore: {ignore_globs}, Include: {include_globs}")

        blames = []
        try:
            # List files at the specified revision
            file_output = self.repo.git.ls_tree("-r", "--name-only", rev)
            # Correct split character to standard newline
            file_names = [f for f in file_output.split("\n") if f.strip()]
        except GitCommandError as e:
            logger.error(f"Could not list files for rev '{rev}': {e}")
            return DataFrame()  # Return empty DataFrame if we can't list files

        for file in self.__check_extension(
            {x: x for x in file_names},
            ignore_globs=ignore_globs,
            include_globs=include_globs,
        ):
            try:
                logger.debug(f"Getting blame for file: {file} at rev: {rev}")
                # Use the relative path directly from ls-tree
                blame_output = self.repo.blame(rev, file)
                for commit, lines in blame_output:
                    # Store the relative path directly
                    blames.append((commit, lines, file))
            except GitCommandError as e:
                logger.warning(f"Failed to get blame for file: {file} at rev: {rev}. Error: {e}")
                continue
            except UnicodeDecodeError as e:
                logger.warning(f"Skipping binary file that cannot be decoded: {file} at rev: {rev}. Error: {e}")
                continue

        if committer:
            if by == "repository":
                blames_df = (
                    DataFrame(
                        [[x[0].committer.name, len(x[1])] for x in blames],
                        columns=["committer", "loc"],
                    )
                    .groupby("committer")["loc"]
                    .sum()
                    .to_frame()
                )
            elif by == "file":
                blames_df = (
                    DataFrame(
                        [[x[0].committer.name, len(x[1]), x[2]] for x in blames],
                        columns=["committer", "loc", "file"],
                    )
                    .groupby(["committer", "file"])["loc"]
                    .sum()
                    .to_frame()
                )
        else:
            if by == "repository":
                blames_df = (
                    DataFrame(
                        [[x[0].author.name, len(x[1])] for x in blames],
                        columns=["author", "loc"],
                    )
                    .groupby("author")["loc"]
                    .sum()
                    .to_frame()
                )
            elif by == "file":
                blames_df = (
                    DataFrame(
                        [[x[0].author.name, len(x[1]), x[2]] for x in blames],
                        columns=["author", "loc", "file"],
                    )
                    .groupby(["author", "file"])["loc"]
                    .sum()
                    .to_frame()
                )

        blames_df = self._add_labels_to_df(blames_df)

        logger.info(f"Finished calculating blame for rev '{rev}'. Found {len(blames_df)} blame entries.")
        return blames_df

    @multicache(key_prefix="revs", key_list=["branch", "limit", "skip", "num_datapoints"])
    def revs(self, branch=None, limit=None, skip=None, num_datapoints=None, skip_broken=False):
        """
        Returns a dataframe of all revision tags and their timestamps. It will have the columns:

         * date
         * rev

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of revisions to return, None for no limit
            skip (Optional[int]): Number of revisions to skip. Ex: skip=2 returns every other
                revision, None for no skipping.
            num_datapoints (Optional[int]): If limit and skip are none, and this isn't, then
                num_datapoints evenly spaced revs will be used
            skip_broken (bool): Whether to skip corrupted commit objects. Defaults to False.

        Returns:
            DataFrame: DataFrame with revision information
        """
        if branch is None:
            branch = self.default_branch

        logger.info(
            f"Fetching revisions for branch '{branch}'. Limit: {limit}, Skip: {skip}, "
            f"Num Datapoints: {num_datapoints}, Skip Broken: {skip_broken}"
        )

        if limit is None and skip is None and num_datapoints is not None:
            logger.debug("Calculating skip based on num_datapoints")
            try:
                # Safely count commits
                commit_count = 0
                for _ in self.repo.iter_commits(branch):
                    commit_count += 1
                limit = commit_count
                skip = int(float(limit) / num_datapoints) if commit_count > 0 else 1
                logger.debug(f"Calculated limit={limit}, skip={skip} from {commit_count} commits")
            except git.exc.GitCommandError as e:
                logger.error(f"Error counting commits for branch '{branch}': {e}")
                return pd.DataFrame(columns=["date", "rev"])
        else:
            if limit is None:
                limit = None  # Let Git handle unlimited commits naturally
            elif skip is not None:
                limit = limit * skip

        ds = []
        skipped_count = 0
        try:
            commits_iterator = self.repo.iter_commits(branch, max_count=limit)
            for commit in commits_iterator:
                try:
                    # Get required properties safely
                    try:
                        # Capture all needed data in a single access to avoid file handle issues
                        committed_date = commit.committed_date
                        name_rev = commit.name_rev

                        # Safely handle name_rev format
                        parts = name_rev.split(" ") if name_rev else []
                        rev_sha = parts[0] if parts else commit.hexsha

                        ds.append([committed_date, rev_sha])
                    except (ValueError, AttributeError) as e:
                        if skip_broken:
                            logger.warning(
                                f"Skipping commit {commit.hexsha if hasattr(commit, 'hexsha') else 'unknown'}: {e}"
                            )
                            skipped_count += 1
                            continue
                        else:
                            logger.error(f"Error processing commit: {e}")
                            raise
                except git.exc.GitCommandError as git_err:
                    if skip_broken:
                        logger.warning(f"Skipping commit due to Git error: {git_err}")
                        skipped_count += 1
                        continue
                    else:
                        logger.error(f"Git error processing commit: {git_err}")
                        raise
                except Exception as e:
                    if skip_broken:
                        logger.warning(f"Skipping commit due to unexpected error: {e}")
                        skipped_count += 1
                        continue
                    else:
                        logger.error(f"Unexpected error processing commit: {e}")
                        raise
        except git.exc.GitCommandError as e:
            logger.error(f"Could not iterate commits for branch '{branch}' in revs(): {e}")
            # Return empty DataFrame if iteration fails
            return pd.DataFrame(columns=["date", "rev"])

        if not ds:
            logger.warning(f"No valid revisions found for branch '{branch}'")
            return pd.DataFrame(columns=["date", "rev"])

        df = DataFrame(ds, columns=["date", "rev"])

        if skip is not None:
            logger.debug(f"Applying skip ({skip}) to revisions.")
            if skip == 0:
                skip = 1

            if df.shape[0] >= skip:
                df = df.iloc[range(0, df.shape[0], skip)]
                df.reset_index(drop=True, inplace=True)
            else:
                df = df.iloc[[0]]
                df.reset_index(drop=True, inplace=True)

        df = self._add_labels_to_df(df)

        if skipped_count > 0:
            logger.info(
                f"Finished fetching revisions for '{branch}'. Found {len(df)} "
                f"valid revisions, skipped {skipped_count} corrupted objects."
            )
        else:
            logger.info(f"Finished fetching revisions for '{branch}'. Found {len(df)} revisions.")
        return df

    @multicache(
        key_prefix="cumulative_blame",
        key_list=["branch", "limit", "skip", "num_datapoints", "committer", "ignore_globs", "include_globs"],
    )
    def cumulative_blame(
        self,
        branch=None,
        limit=None,
        skip=None,
        num_datapoints=None,
        committer=True,
        ignore_globs=None,
        include_globs=None,
        skip_broken=True,
    ):
        """
        Returns the blame at every revision of interest. Index is a datetime, column per
        committer, with number of lines blamed to each committer at each timestamp as data.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of revisions to return, None for no limit
            skip (Optional[int]): Number of revisions to skip. Ex: skip=2 returns every other
                revision, None for no skipping.
            num_datapoints (Optional[int]): If limit and skip are none, and this isn't, then
                num_datapoints evenly spaced revs will be used
            committer (bool, optional): True if committer should be reported, false if author
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include
            skip_broken (bool, optional): Whether to skip corrupted Git objects. Defaults to True.

        Returns:
            DataFrame: DataFrame with blame information

        Note:
            If both ignore_globs and include_globs are provided, files must match an include
            pattern and not match any ignore patterns to be included.
        """
        if branch is None:
            branch = self.default_branch

        logger.info(
            f"Starting cumulative blame calculation for branch '{branch}'. "
            f"Limit: {limit}, Skip: {skip}, Num Datapoints: {num_datapoints}, "
            f"Committer: {committer}, Skip Broken: {skip_broken}"
        )

        # Pass skip_broken and force_refresh to ensure robustness when getting revisions
        revs = self.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints, skip_broken=skip_broken)

        # Check immediately after calling revs()
        if not revs.empty and "rev" not in revs.columns:
            logger.error("DataFrame returned from self.revs() is missing the 'rev' column.")
            # Raise a specific error to make it clear.
            raise ValueError("Internal Error: self.revs() returned DataFrame without 'rev' column.")

        # get the commit history to stub out committers (hacky and slow)
        logger.debug("Fetching all committers to pre-populate columns...")
        committers = set()
        try:
            for commit in self.repo.iter_commits(branch):
                try:
                    # Determine the name based on the 'committer' flag
                    name = commit.committer.name if committer else commit.author.name
                    committers.add(name)
                except ValueError as e:
                    # Handle potential errors resolving commit objects (e.g., due to corruption)
                    logger.warning(
                        f"Could not resolve commit object "
                        f"{commit.hexsha if hasattr(commit, 'hexsha') else 'unknown'} when fetching committers: {e}"
                    )
                    continue
                except Exception as e:
                    # Catch other potential errors getting name (e.g., missing name)
                    logger.warning(
                        f"Error getting committer/author name for commit "
                        f"{commit.hexsha if hasattr(commit, 'hexsha') else 'unknown'}: {e}"
                    )
                    continue
        except GitCommandError as e:
            logger.error(f"Could not iterate commits for branch '{branch}' to get committers: {e}")
            # Return empty DataFrame if we can't even get committers
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

        # Check if any committers were found
        if not committers:
            logger.warning(f"No valid committers found for branch '{branch}'. Returning empty DataFrame.")
            # Return an empty DataFrame with a 'date' index to avoid errors downstream
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

        # If revs is empty, return an empty DataFrame with proper index
        if revs.empty:
            logger.warning(f"No valid revisions found for branch '{branch}'. Returning empty DataFrame.")
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

        for y in committers:
            revs[y] = 0

        if self.verbose:
            print("Beginning processing for cumulative blame:")
        logger.debug(f"Processing {len(revs)} revisions for cumulative blame...")

        # now populate that table with some actual values
        for idx, row in revs.iterrows():
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Processing blame for rev: {row['rev']} (Index: {idx})")

            try:
                blame = self.blame(
                    rev=row["rev"],
                    committer=committer,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                for y in committers:
                    try:
                        loc = blame.loc[y, "loc"]
                        revs.at[idx, y] = loc
                    except KeyError:
                        pass
            except GitCommandError as e:
                logger.warning(f"Skipping blame for revision {row['rev']}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Unexpected error processing blame for revision {row['rev']}: {e}")
                continue

        # If revs is now empty after processing, return an empty DataFrame
        if revs.empty:
            logger.warning("No valid blame data found after processing. Returning empty DataFrame.")
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

        try:
            del revs["rev"]

            # Convert date strings to numeric type before using to_datetime
            revs["date"] = pd.to_numeric(revs["date"])
            revs["date"] = pd.to_datetime(revs["date"], unit="s", utc=True)
            revs.set_index(keys=["date"], drop=True, inplace=True)
            revs = revs.fillna(0.0)

            # drop 0 cols
            for col in revs.columns.values:
                if col != "col" and revs[col].sum() == 0:
                    del revs[col]

            # drop 0 rows
            keep_idx = []
            committers = [x for x in revs.columns.values if x != "date"]
            for idx, row in revs.iterrows():
                # Convert any string values to numeric, treating non-numeric strings as 0
                row_sum = 0
                for x in committers:
                    try:
                        val = float(row[x])
                        row_sum += val
                    except (ValueError, TypeError):
                        continue
                if row_sum > 0:
                    keep_idx.append(idx)

            logger.debug(f"Filtering complete. Kept {len(keep_idx)} non-zero rows.")

            # Only filter if we have rows to keep
            if keep_idx:
                revs = revs.loc[keep_idx]
        except Exception as e:
            logger.error(f"Error processing cumulative blame data: {e}")
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

        logger.info(f"Finished cumulative blame calculation for '{branch}'. Result shape: {revs.shape}")
        return revs

    @multicache(
        key_prefix="parallel_cumulative_blame",
        key_list=["branch", "limit", "skip", "num_datapoints", "committer", "workers", "ignore_globs", "include_globs"],
    )
    def parallel_cumulative_blame(
        self,
        branch=None,
        limit=None,
        skip=None,
        num_datapoints=None,
        committer=True,
        workers=1,
        ignore_globs=None,
        include_globs=None,
        skip_broken=True,
    ):
        """
        Returns the blame at every revision of interest. Index is a datetime, column per
        committer, with number of lines blamed to each committer at each timestamp as data.

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of revisions to return, None for no limit
            skip (Optional[int]): Number of revisions to skip. Ex: skip=2 returns every other
                revision, None for no skipping.
            num_datapoints (Optional[int]): If limit and skip are none, and this isn't, then
                num_datapoints evenly spaced revs will be used
            committer (bool, optional): True if committer should be reported, false if author
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include
            workers (Optional[int]): Number of workers to use in the threadpool, -1 for one per core.
            skip_broken (bool, optional): Whether to skip corrupted Git objects. Defaults to True.

        Returns:
            DataFrame: DataFrame with blame information
        """
        if branch is None:
            branch = self.default_branch

        logger.info(
            f"Starting parallel cumulative blame for branch '{branch}'. "
            f"Limit: {limit}, Skip: {skip}, Num Datapoints: {num_datapoints}, "
            f"Committer: {committer}, Workers: {workers}, Skip Broken: {skip_broken}"
        )

        if not _has_joblib:
            logger.error("Joblib not installed. Cannot run parallel_cumulative_blame.")
            raise ImportError("""Must have joblib installed to use parallel_cumulative_blame(), please use
            cumulative_blame() instead.""")

        revs = self.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints, skip_broken=skip_broken)

        # If revs is empty, return an empty DataFrame with proper index
        if revs.empty:
            logger.warning(f"No valid revisions found for branch '{branch}'. Returning empty DataFrame.")
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

        logger.debug(f"Prepared {len(revs)} revisions for parallel processing.")

        try:
            revisions = json.loads(revs.to_json(orient="index"))
            revisions = [revisions[key] for key in revisions]

            ds = Parallel(n_jobs=workers, backend="threading", verbose=5)(
                delayed(_parallel_cumulative_blame_func)(self, x, committer, ignore_globs, include_globs)
                for x in revisions
            )

            if not ds:
                logger.warning("No valid blame data found after processing. Returning empty DataFrame.")
                return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

            revs = DataFrame(ds)
            del revs["rev"]

            # Convert date strings to numeric type before using to_datetime
            revs["date"] = pd.to_numeric(revs["date"])
            revs["date"] = pd.to_datetime(revs["date"], unit="s", utc=True)
            revs.set_index(keys=["date"], drop=True, inplace=True)
            revs = revs.fillna(0.0)

            # drop 0 cols
            for col in revs.columns.values:
                if col != "col" and revs[col].sum() == 0:
                    del revs[col]

            # drop 0 rows
            keep_idx = []
            committers = [x for x in revs.columns.values if x != "date"]
            for idx, row in revs.iterrows():
                # Convert any string values to numeric, treating non-numeric strings as 0
                row_sum = 0
                for x in committers:
                    try:
                        val = float(row[x])
                        row_sum += val
                    except (ValueError, TypeError):
                        continue
                if row_sum > 0:
                    keep_idx.append(idx)

            logger.debug(f"Filtering complete. Kept {len(keep_idx)} non-zero rows.")

            # Only filter if we have rows to keep
            if keep_idx:
                revs = revs.loc[keep_idx]

            logger.info(f"Finished parallel cumulative blame for '{branch}'. Result shape: {revs.shape}")
            return revs
        except Exception as e:
            logger.error(f"Error in parallel cumulative blame: {e}")
            return pd.DataFrame(index=pd.to_datetime([]).tz_localize("UTC"))

    @multicache(key_prefix="branches", key_list=[])
    def branches(self):
        """Returns information about all branches in the repository.

        Retrieves a list of all branches (both local and remote) from the repository.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Repository name
                - branch (str): Name of the branch
                - local (bool): Whether the branch is local
                Additional columns for any labels specified in labels_to_add
        """

        logger.info("Fetching repository branches (local and remote).")

        # first pull the local branches
        logger.debug("Fetching local branches...")
        local_branches = self.repo.branches
        data = [[x.name, True] for x in list(local_branches)]

        # then the remotes
        logger.debug("Fetching remote branches...")
        remote_branches = self.repo.git.branch("-r").replace(" ", "").splitlines()
        rb = []
        for _i, remote in enumerate(remote_branches):
            if "->" in remote:
                continue
            # Strip origin/ prefix
            if remote.startswith("origin/"):
                remote = remote[7:]
            rb.append(remote)
        remote_branches = set(rb)

        data += [[x, False] for x in remote_branches]

        df = DataFrame(data, columns=["branch", "local"])
        df = self._add_labels_to_df(df)

        logger.info(f"Finished fetching branches. Found {len(df)} total branches.")
        return df

    @multicache(key_prefix="get_branches_by_commit", key_list=["commit"])
    def get_branches_by_commit(self, commit):
        """Finds all branches containing a specific commit.

        Args:
            commit (str): Commit hash to look up

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - branch (str): Name of each branch containing the commit
                - commit (str): The commit hash that was looked up
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add
        """
        logger.info(f"Finding branches containing commit: {commit}")
        branches = self.repo.git.branch("-a", "--contains", commit).replace(" ", "").replace("*", "").splitlines()
        df = DataFrame(branches, columns=["branch"])
        df["commit"] = str(commit)
        df = self._add_labels_to_df(df)

        logger.info(f"Found {len(df)} branches containing commit {commit}.")
        return df

    @multicache(key_prefix="commits_in_tags", key_list=["start", "end"])
    def commits_in_tags(self, start=None, end=None):
        """Analyzes commits associated with each tag.

        For each tag, traces backwards through the commit history until hitting another
        tag, reaching the time limit, or hitting the root commit. This helps understand
        what changes went into each tagged version.

        Args:
            start (Union[np.timedelta64, pd.Timestamp], optional): Start time for analysis.
                If a timedelta, calculated relative to now. Defaults to 6 months ago.
            end (Optional[pd.Timestamp]): End time for analysis. Defaults to None.

        Returns:
            pandas.DataFrame: A DataFrame indexed by (tag_date, commit_date) with columns:
                - commit_sha (str): SHA of the commit
                - tag (str): Name of the tag this commit belongs to
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            This is useful for generating changelogs or understanding the scope
            of changes between tagged releases.
        """
        logger.info(f"Analyzing commits within tags. Start: {start}, End: {end}")

        if start is None:
            start = np.timedelta64(180, "D")  # Approximately 6 months

        # If we pass in a timedelta instead of a timestamp, calc the timestamp relative to now
        if isinstance(start, pd.Timedelta | np.timedelta64):
            start = pd.Timestamp.today(tz="UTC") - start
        if isinstance(end, pd.Timedelta | np.timedelta64):
            end = pd.Timestamp.today(tz="UTC") - end

        # remove tagged commits outside our date ranges
        df_tags = self.tags()
        if start:
            df_tags = df_tags.query(f'commit_date > "{start}"').copy()
        if end:
            df_tags = df_tags.query(f'commit_date < "{end}"').copy()

        # convert to unix time to speed up calculations later
        start = (start - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1s") if start else start
        end = (end - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1s") if end else end

        ds = []
        checked_commits = set()

        df_tags["filled_shas"] = df_tags["tag_sha"].fillna(value=df_tags["commit_sha"])
        logger.debug(f"Processing {len(df_tags)} tags within the specified date range.")
        for sha, tag_name in df_tags[["filled_shas", "tag"]].sort_index(level="tag_date").values:
            logger.debug(f"Processing tag '{tag_name}' starting from SHA: {sha}")
            commit = self.repo.commit(sha)
            before_start = start and commit.committed_date < start
            passed_end = end and commit.committed_date > end
            already_checked = str(commit) in checked_commits
            if before_start or passed_end or already_checked:
                continue
            tag = self.repo.tag(tag_name)

            checked_commits.add(str(commit))
            logger.debug(f"Adding commit {commit.hexsha[:7]} for tag '{tag.name}'")
            ds.append(self._commits_per_tags_helper(commit, df_tags, tag=tag))

        if not ds:
            logger.info("No commits found within tags for the specified range.")
            return pd.DataFrame(columns=["commit_sha", "tag", "tag_date", "commit_date"])

        df = pd.DataFrame(ds)
        df = df.set_index(["tag_date", "commit_date"])
        df = self._add_labels_to_df(df)

        logger.info(f"Finished analyzing commits in tags. Found {len(df)} commits.")
        return df

    def _commits_per_tags_recursive(
        self,
        commit,
        df_tags,
        ds=None,
        tag=None,
        checked_commits=None,
        start=None,
        end=None,
    ):
        logger.debug(f"Recursive check for commit {commit.hexsha[:7]} under tag '{tag.name if tag else None}'")
        ds = ds if ds is not None else []
        checked_commits = checked_commits if checked_commits is not None else set()

        for parent_commit in commit.parents:
            before_start = start and parent_commit.committed_date < start
            passed_end = end and parent_commit.committed_date > end
            already_checked = str(parent_commit) in checked_commits
            if before_start or passed_end or already_checked:
                logger.debug(
                    f"Skipping parent commit {parent_commit.hexsha[:7]}: BeforeStart={before_start}, PassedEnd={passed_end}, AlreadyChecked={already_checked}"  # noqa: E501
                )
                continue
            checked_commits.add(str(parent_commit))
            commit_meta, tag = self._commits_per_tags_helper(commit=parent_commit, df_tags=df_tags, tag=tag)
            ds.append(commit_meta)
            self._commits_per_tags_recursive(
                commit=parent_commit,
                df_tags=df_tags,
                ds=ds,
                tag=tag,
                checked_commits=checked_commits,
                start=start,
                end=end,
            )

    def _commits_per_tags_helper(self, commit, df_tags, tag=None):
        tag_pd = df_tags.loc[
            (df_tags["commit_sha"].str.contains(str(commit))) | (df_tags["tag_sha"].str.contains(str(commit)))
        ].tag
        if not tag_pd.empty:
            tag = self.repo.tag(tag_pd[0])
        tag_date = tag.tag.tagged_date if tag and tag.tag else commit.committed_date
        tag_date = pd.to_datetime(tag_date, unit="s", utc=True)
        commit_date = pd.to_datetime(commit.committed_date, unit="s", utc=True)

        return {
            "commit_sha": str(commit),
            "tag": str(tag),
            "tag_date": tag_date,
            "commit_date": commit_date,
        }

    @multicache(key_prefix="tags", key_list=[])
    def tags(self, skip_broken=False):
        """Returns information about all tags in the repository.

        Retrieves detailed information about all tags, including both lightweight
        and annotated tags.

        Args:
            skip_broken (bool): Whether to skip corrupted tag objects. Defaults to False.

        Returns:
            pandas.DataFrame: A DataFrame indexed by (tag_date, commit_date) with columns:
                - tag (str): Name of the tag
                - annotated (bool): Whether it's an annotated tag
                - annotation (str): Tag message (empty for lightweight tags)
                - tag_sha (Optional[str]): SHA of tag object (None for lightweight tags)
                - commit_sha (str): SHA of the commit being tagged
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            - tag_date is the tag creation time for annotated tags, commit time for lightweight
            - commit_date is always the timestamp of the tagged commit
            - Both dates are timezone-aware UTC timestamps
        """
        logger.info(f"Fetching repository tags (skip_broken={skip_broken}).")

        tags = self.repo.tags
        tags_meta = []
        cols = [
            "tag_date",
            "commit_date",
            "tag",
            "annotated",
            "annotation",
            "tag_sha",
            "commit_sha",
        ]

        skipped_count = 0
        for tag in tags:
            try:
                d = dict.fromkeys(cols)
                d["tag"] = tag.name

                # Safely handle tag object access
                tag_obj = None
                try:
                    # Check if this is an annotated tag (has tag object)
                    tag_obj = tag.tag
                except (ValueError, AttributeError, git.exc.GitCommandError):
                    # Not an annotated tag or tag object is inaccessible
                    tag_obj = None

                if tag_obj is not None:
                    # This is a safer way to access tag properties - get all at once
                    try:
                        # Store all tag object attributes we need in one go
                        d["annotated"] = True
                        d["tag_date"] = str(tag_obj.tagged_date)
                        d["annotation"] = str(tag_obj.message)
                        d["tag_sha"] = str(tag_obj.hexsha)
                    except (ValueError, AttributeError, git.exc.GitCommandError) as e:
                        if skip_broken:
                            logger.warning(f"Skipping corrupted tag object '{tag.name}': {e}")
                            skipped_count += 1
                            continue
                        else:
                            logger.error(f"Error accessing tag object '{tag.name}': {e}")
                            raise
                else:
                    # Lightweight tag
                    d["annotated"] = False
                    d["annotation"] = ""
                    d["tag_sha"] = None

                # Safely get commit information
                try:
                    commit = tag.commit
                    d["commit_date"] = commit.committed_date
                    d["commit_sha"] = commit.hexsha

                    # For lightweight tags, use commit date as tag date
                    if "tag_date" not in d or d["tag_date"] is None:
                        d["tag_date"] = commit.committed_date
                except (ValueError, git.exc.GitCommandError) as e:
                    if skip_broken:
                        logger.warning(f"Skipping tag '{tag.name}' with invalid commit reference: {e}")
                        skipped_count += 1
                        continue
                    else:
                        logger.error(f"Error accessing commit for tag '{tag.name}': {e}")
                        raise

                tags_meta.append(d)
            except git.exc.GitCommandError as git_err:
                # Handle Git command errors (like unknown object type)
                if skip_broken:
                    logger.warning(f"Skipping tag '{tag.name}' due to Git error: {git_err}")
                    skipped_count += 1
                    continue
                else:
                    logger.error(f"Git error reading tag '{tag.name}': {git_err}")
                    raise
            except ValueError as ve:
                # Handle file handle errors and value errors
                if skip_broken:
                    logger.warning(f"Skipping tag '{tag.name}' due to value error: {ve}")
                    skipped_count += 1
                    continue
                else:
                    logger.error(f"Value error while reading tag '{tag.name}': {ve}")
                    raise
            except Exception as e:
                # General error handling
                if skip_broken:
                    logger.warning(f"Skipping tag '{tag.name}' due to unexpected error: {e}")
                    skipped_count += 1
                    continue
                else:
                    logger.error(f"Unexpected error while processing tag '{tag.name}': {e}")
                    raise

        if not tags_meta:
            logger.info("No valid tags found in the repository.")
            # Return an empty DataFrame with the expected columns
            df = DataFrame(columns=cols)
            df = self._add_labels_to_df(df)
            return df

        df = DataFrame(tags_meta, columns=cols)

        df["tag_date"] = to_datetime(pd.to_numeric(df["tag_date"], errors="coerce"), unit="s", utc=True)
        df["commit_date"] = to_datetime(pd.to_numeric(df["commit_date"], errors="coerce"), unit="s", utc=True)
        df = self._add_labels_to_df(df)

        df = df.set_index(keys=["tag_date", "commit_date"], drop=True)
        df = df.sort_index(level=["tag_date", "commit_date"])

        if skipped_count > 0:
            logger.info(f"Finished fetching tags. Found {len(df)} valid tags, skipped {skipped_count} corrupted tags.")
        else:
            logger.info(f"Finished fetching tags. Found {len(df)} tags.")
        return df

    @property
    def repo_name(self):
        return self._repo_name()

    def _repo_name(self):
        """Returns the name of the repository.

        For local repositories, uses the name of the directory containing the .git folder.
        For remote repositories, extracts the name from the URL.

        Returns:
            str: Name of the repository, or 'unknown_repo' if name can't be determined

        Note:
            This is an internal method primarily used to provide consistent repository
            names in DataFrame outputs.
        """

        if self._git_repo_name is not None:
            return self._git_repo_name
        else:
            reponame = self.repo.git_dir.split(os.sep)[-2]
            if reponame.strip() == "":
                return "unknown_repo"
            return reponame

    def _add_labels_to_df(self, df):
        """Adds configured labels to a DataFrame.

        Adds the repository name and any additional configured labels to the DataFrame.
        This ensures consistent labeling across all DataFrame outputs.

        Args:
            df (pandas.DataFrame): DataFrame to add labels to

        Returns:
            pandas.DataFrame: The input DataFrame with additional label columns:
                - repository (str): Repository name
                - label0..labelN: Values from labels_to_add

        Note:
            This is an internal helper method used by all public methods that
            return DataFrames.
        """
        df["repository"] = self._repo_name()
        for i, label in enumerate(self._labels_to_add):
            df[f"label{i}"] = label
        return df

    def __str__(self):
        """Returns a human-readable string representation of the repository.

        Returns:
            str: String in format 'git repository: {name} at: {path}'
        """
        return f"git repository: {self._repo_name()} at: {self.git_dir}"

    @multicache(key_prefix="get_commit_content", key_list=["rev", "ignore_globs", "include_globs"])
    def get_commit_content(self, rev, ignore_globs=None, include_globs=None):
        """Gets detailed content changes for a specific commit.

        For each file changed in the commit, returns the actual content changes
        including added and removed lines.

        Args:
            rev (str): Revision (commit hash) to analyze
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - file (str): Path of the changed file
                - change_type (str): Type of change (A=added, M=modified, D=deleted)
                - old_line_num (int): Line number in the old version (None for added lines)
                - new_line_num (int): Line number in the new version (None for deleted lines)
                - content (str): The actual line content
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            For binary files, only the change_type is recorded, with no line-by-line changes.
            If both ignore_globs and include_globs are provided, files must match an include
            pattern and not match any ignore patterns to be included.
        """
        logger.info(f"Getting detailed content changes for revision '{rev}'")

        try:
            commit = self.repo.commit(rev)

            # Get the parent commit. For merge commits, use first parent
            parent = commit.parents[0] if commit.parents else None
            parent_sha = parent.hexsha if parent else "4b825dc642cb6eb9a060e54bf8d69288fbee4904"  # empty tree

            # Get the diff between this commit and its parent
            diff = self.repo.git.diff(
                parent_sha,
                commit.hexsha,
                "--unified=0",  # No context lines
                "--no-prefix",  # Don't prefix with a/ and b/
                "--no-renames",  # Don't try to detect renames
            )

            changes = []
            current_file = None
            current_type = None

            for line in diff.split("\n"):
                if line.startswith("diff --git"):
                    # New file being processed
                    file_path = line.split(" ")[-1]

                    # Check if this file should be included based on globs
                    if not self.__check_extension({file_path: None}, ignore_globs, include_globs):
                        current_file = None
                        continue

                    current_file = file_path

                elif line.startswith("new file"):
                    current_type = "A"
                elif line.startswith("deleted"):
                    current_type = "D"
                elif line.startswith("index"):
                    current_type = "M"
                elif line.startswith("@@") and current_file:
                    # Parse the @@ line to get line numbers
                    # Format: @@ -old_start,old_count +new_start,new_count @@
                    nums = line.split("@@")[1].strip().split(" ")
                    old_range = nums[0].split(",")
                    new_range = nums[1].split(",")

                    old_start = int(old_range[0].lstrip("-"))
                    new_start = int(new_range[0].lstrip("+"))

                elif line.startswith("+") and current_file and not line.startswith("+++"):
                    # Added line
                    changes.append(
                        [
                            current_file,
                            current_type,
                            None,  # old line number
                            new_start,
                            line[1:],  # Remove the + prefix
                        ]
                    )
                    new_start += 1

                elif line.startswith("-") and current_file and not line.startswith("---"):
                    # Removed line
                    changes.append(
                        [
                            current_file,
                            current_type,
                            old_start,
                            None,  # new line number
                            line[1:],  # Remove the - prefix
                        ]
                    )
                    old_start += 1

            if not changes:
                logger.info(f"No changes found in revision '{rev}' matching the filters")
                return DataFrame(columns=["file", "change_type", "old_line_num", "new_line_num", "content"])

            df = DataFrame(changes, columns=["file", "change_type", "old_line_num", "new_line_num", "content"])
            df = self._add_labels_to_df(df)

            logger.info(f"Found {len(df)} line changes in revision '{rev}'")
            return df

        except (GitCommandError, IndexError, BadObject, BadName) as e:
            logger.error(f"Failed to get content changes for revision '{rev}': {e}")
            return DataFrame(columns=["file", "change_type", "old_line_num", "new_line_num", "content"])

    @multicache(key_prefix="get_file_content", key_list=["path", "rev"])
    def get_file_content(self, path, rev="HEAD"):
        """Gets the content of a file from the repository at a specific revision.

        Safely retrieves file content by first verifying the file exists in git's
        tree (respecting .gitignore) before attempting to read it.

        Args:
            path (str): Path to the file relative to repository root
            rev (str, optional): Revision to get file from. Defaults to 'HEAD'.

        Returns:
            Optional[str]: Content of the file if it exists and is tracked by git,
                None if file doesn't exist or isn't tracked.

        Note:
            This only works for files that are tracked by git. Untracked files and
            files matched by .gitignore patterns cannot be read.
        """
        logger.info(f"Getting content of file '{path}' at revision '{rev}'")

        try:
            # First verify the file exists in git's tree
            try:
                # ls-tree -r for recursive, --full-name for full paths
                # -l for long format (includes size)
                self.repo.git.ls_tree("-r", "-l", "--full-name", rev, path)
            except GitCommandError:
                logger.warning(f"File '{path}' not found in git tree at revision '{rev}'")
                return None

            # If we get here, the file exists in git's tree
            # Use git show to get the file content
            content = self.repo.git.show(f"{rev}:{path}")
            return content

        except GitCommandError as e:
            logger.error(f"Failed to get content of file '{path}' at revision '{rev}': {e}")
            return None

    @multicache(key_prefix="list_files", key_list=["rev"])
    def list_files(self, rev="HEAD"):
        """Lists all files in the repository at a specific revision, respecting .gitignore.

        Uses git ls-tree to get a list of all tracked files in the repository,
        which automatically respects .gitignore rules since untracked and ignored
        files are not in git's tree.

        Args:
            rev (str, optional): Revision to list files from. Defaults to 'HEAD'.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - file (str): Full path to the file relative to repository root
                - mode (str): File mode (100644 for regular file, 100755 for executable, etc)
                - type (str): Object type (blob for file, tree for directory)
                - sha (str): SHA-1 hash of the file content
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            This only includes files that are tracked by git. Untracked files and
            files matched by .gitignore patterns are not included.
        """
        logger.info(f"Listing files at revision '{rev}'")

        try:
            # Get the full file list with details using ls-tree
            # -r for recursive
            # -l for long format (includes file size)
            # --full-tree to start from root
            # --full-name for full paths
            output = self.repo.git.ls_tree("-r", "-l", "--full-tree", "--full-name", rev)

            if not output.strip():
                logger.info("No files found in repository")
                return DataFrame(columns=["file", "mode", "type", "sha"])

            # Parse the ls-tree output
            # Format: <mode> <type> <sha> <size>\t<file>
            files = []
            for line in output.split("\n"):
                if not line.strip():
                    continue

                # Split on tab first to separate path from rest
                details, path = line.split("\t")
                mode, obj_type, sha, _ = details.split()
                files.append([path, mode, obj_type, sha])

            df = DataFrame(files, columns=["file", "mode", "type", "sha"])
            df = self._add_labels_to_df(df)

            logger.info(f"Found {len(df)} files at revision '{rev}'")
            return df

        except GitCommandError as e:
            logger.error(f"Failed to list files at revision '{rev}': {e}")
            return DataFrame(columns=["file", "mode", "type", "sha"])

    def __repr__(self):
        """Returns a unique string representation of the repository.

        Returns:
            str: The absolute path to the repository
        """
        return str(self.git_dir)

    @multicache(key_prefix="bus_factor", key_list=["by", "ignore_globs", "include_globs"])
    def bus_factor(self, by="repository", ignore_globs=None, include_globs=None):
        """Calculates the "bus factor" for the repository.

        The bus factor is a measure of risk based on how concentrated the codebase knowledge is
        among contributors. It is calculated as the minimum number of contributors whose combined
        contributions account for at least 50% of the codebase's lines of code.

        Args:
            by (str, optional): How to calculate the bus factor. One of:
                - 'repository': Calculate for entire repository (default)
                - 'file': Calculate for each individual file
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns depending on the 'by' parameter:
                If by='repository':
                    - repository (str): Repository name
                    - bus factor (int): Bus factor for the repository
                If by='file':
                    - file (str): File path
                    - bus factor (int): Bus factor for that file
                    - repository (str): Repository name

        Note:
            A low bus factor (e.g. 1-2) indicates high risk as knowledge is concentrated among
            few contributors. A higher bus factor indicates knowledge is better distributed.
        """
        logger.info(f"Calculating bus factor. Group by: {by}, Ignore: {ignore_globs}, Include: {include_globs}")

        if by == "file":
            # Get file-wise blame data
            blame = self.blame(include_globs=include_globs, ignore_globs=ignore_globs, by="file")

            if blame.empty:
                logger.warning("No blame data found for file-wise bus factor calculation.")
                return DataFrame(columns=["file", "bus factor", "repository"])

            # Reset index to access file column if it's in the index
            if isinstance(blame.index, pd.MultiIndex) and "file" in blame.index.names:
                blame = blame.reset_index()

            # Group by file and calculate bus factor for each file
            file_bus_factors = []
            files = blame["file"].unique()

            for file_name in files:
                file_blame = blame[blame["file"] == file_name].copy()
                file_blame = file_blame.sort_values(by=["loc"], ascending=False)

                total = file_blame["loc"].sum()
                if total == 0:
                    # If file has no lines of code, skip it
                    continue

                cumulative = 0
                tc = 0
                for idx in range(file_blame.shape[0]):
                    cumulative += file_blame.iloc[idx]["loc"]
                    tc += 1
                    if cumulative >= total / 2:
                        break

                file_bus_factors.append([file_name, tc, self._repo_name()])

            logger.info(f"Calculated bus factor for {len(file_bus_factors)} files.")
            return DataFrame(file_bus_factors, columns=["file", "bus factor", "repository"])

        blame = self.blame(include_globs=include_globs, ignore_globs=ignore_globs, by=by)
        blame = blame.sort_values(by=["loc"], ascending=False)

        total = blame["loc"].sum()
        cumulative = 0
        tc = 0
        for idx in range(blame.shape[0]):
            cumulative += blame.iloc[idx]["loc"]
            tc += 1
            if cumulative >= total / 2:
                break

        logger.info(f"Bus factor calculated: {tc}")
        return DataFrame([[self._repo_name(), tc]], columns=["repository", "bus factor"])

    @multicache(key_prefix="file_owner", key_list=["rev", "filename", "committer"])
    def file_owner(self, rev, filename, committer=True):
        """Determines the primary owner of a file at a specific revision.

        The owner is determined by who has contributed the most lines of code
        to the file according to git blame.

        Args:
            rev (str): Revision to analyze
            filename (str): Path to the file relative to repository root
            committer (bool, optional): If True, use committer info. If False, use author.
                Defaults to True.

        Returns:
            Optional[dict]: Dictionary containing owner information with keys:
                - name (str): Name of the primary owner
                Returns None if file doesn't exist or can't be analyzed

        Note:
            This is a helper method used by file_detail() to determine file ownership.
        """
        logger.debug(f"Determining file owner for: {filename} at rev: {rev}, Committer: {committer}")
        try:
            cm = "committer" if committer else "author"

            blame = self.repo.blame(rev, os.path.join(self.git_dir, filename))
            blame = (
                DataFrame(
                    [[x[0].committer.name, len(x[1])] for x in blame],
                    columns=[cm, "loc"],
                )
                .groupby(cm)
                .agg({"loc": "sum"})
            )
            if blame.shape[0] > 0:
                owner = blame["loc"].idxmax()
                return {"name": owner}
            else:
                logger.debug(f"No blame information found for file {filename} at rev {rev}.")
                return None
        except (GitCommandError, KeyError) as e:
            logger.warning(f"Could not determine file owner for {filename} at rev {rev}: {e}")
            return None

    def _get_last_edit_date(self, file_path, rev="HEAD"):
        """Get the last edit date for a file at a given revision.

        Args:
            file_path (str): Path to the file
            rev (str): Revision to check

        Returns:
            datetime: Last edit date for the file
        """
        try:
            cmd = ["git", "log", "-1", "--format=%aI", rev, "--", file_path]
            date_str = self.repo.git.execute(cmd)
            if date_str:
                # Parse ISO 8601 format which includes timezone
                return pd.to_datetime(date_str.strip(), utc=True)
            return pd.NaT
        except Exception as e:
            logger.warning(f"Error getting last edit date for {file_path}: {e}")
            return pd.NaT

    @multicache(
        key_prefix="punchcard", key_list=["branch", "limit", "days", "by", "normalize", "ignore_globs", "include_globs"]
    )
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
        Returns a pandas DataFrame containing all of the data for a punchcard.

         * day_of_week
         * hour_of_day
         * author / committer
         * lines
         * insertions
         * deletions
         * net

        Args:
            branch (Optional[str]): Branch to analyze. Defaults to default_branch if None.
            limit (Optional[int]): Maximum number of commits to return, None for no limit
            days (Optional[int]): Number of days to return if limit is None
            by (Optional[str]): Agg by options, None for no aggregation (just a high level punchcard), or
                'committer', 'author'
            normalize (Optional[int]): If an integer, returns the data normalized to max value of
                that (for plotting)
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            DataFrame: DataFrame with punchcard data
        """
        logger.info(
            f"Generating punchcard data for branch '{branch}'. "
            f"Limit: {limit}, Days: {days}, By: {by}, Normalize: {normalize}, "
            f"Ignore: {ignore_globs}, Include: {include_globs}"
        )

        if branch is None:
            branch = self.default_branch

        logger.debug("Fetching commit history for punchcard...")
        ch = self.commit_history(
            branch=branch,
            limit=limit,
            days=days,
            ignore_globs=ignore_globs,
            include_globs=include_globs,
        )

        # add in the date fields
        ch["day_of_week"] = ch.index.map(lambda x: x.weekday())
        ch["hour_of_day"] = ch.index.map(lambda x: x.hour)

        aggs = ["hour_of_day", "day_of_week"]
        if by is not None:
            aggs.append(by)

        logger.debug(f"Aggregating punchcard data by: {aggs}")
        punch_card = ch.groupby(aggs).agg({"lines": "sum", "insertions": "sum", "deletions": "sum", "net": "sum"})
        punch_card.reset_index(inplace=True)

        # normalize all cols
        if normalize is not None:
            logger.debug(f"Normalizing punchcard data to max value: {normalize}")
            for col in ["lines", "insertions", "deletions", "net"]:
                punch_card[col] = (punch_card[col] / punch_card[col].sum()) * normalize

        logger.info(f"Finished generating punchcard data for '{branch}'. Result shape: {punch_card.shape}")
        return punch_card

    @multicache(key_prefix="has_branch", key_list=["branch"])
    def has_branch(self, branch):
        """Checks if a branch exists in the repository.

        Args:
            branch (str): Name of the branch to check

        Returns:
            bool: True if the branch exists, False otherwise

        Note:
            This checks both local and remote branches.
        """
        logger.info(f"Checking if branch '{branch}' exists.")
        try:
            # Get all branches (both local and remote)
            branches = self.branches()
            result = branch in branches["branch"].values
            logger.info(f"Branch '{branch}' exists: {result}")
            return result
        except GitCommandError as e:
            logger.warning(f"Could not check branches in repo '{self._repo_name()}': {e}")
            return False

    @multicache(key_prefix="file_detail", key_list=["include_globs", "ignore_globs", "rev", "committer"])
    def file_detail(self, include_globs=None, ignore_globs=None, rev="HEAD", committer=True):
        """Provides detailed information about all files in the repository.

        Analyzes each file at the specified revision, gathering information about
        size, ownership, and last modification.

        Args:
            include_globs (Optional[List[str]]): List of glob patterns for files to include
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            rev (str, optional): Revision to analyze. Defaults to 'HEAD'.
            committer (bool, optional): If True, use committer info. If False, use author.
                Defaults to True.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - file (str): Path to the file
                - file_owner (str): Name of primary committer/author
                - last_edit_date (datetime): When file was last modified
                - loc (int): Lines of code in file
                - ext (str): File extension
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            The primary file owner is the person responsible for the most lines
            in the current version of the file.

            This method is cached if a cache_backend was provided and rev is not HEAD.
        """
        logger.info(
            f"Fetching file details for rev '{rev}'. "
            f"Ignore: {ignore_globs}, Include: {include_globs}, Committer: {committer}"
        )

        # first get the blame
        logger.debug("Calculating blame for file details...")
        blame = self.blame(
            include_globs=include_globs,
            ignore_globs=ignore_globs,
            rev=rev,
            committer=committer,
            by="file",
        )
        blame = blame.reset_index(level=-1)
        blame = blame.reset_index(level=-1)

        # reduce it to files and total LOC
        logger.debug("Reducing to files and total LOC...")
        df = blame.reindex(columns=["file", "loc"])
        df = df.groupby("file").agg({"loc": "sum"}).reset_index()  # Keep file as column

        # map in file owners
        logger.debug("Mapping file owners...")

        def _get_owner_name_safe(file_path):
            owner_info = self.file_owner(rev, file_path, committer=committer)
            return owner_info.get("name") if owner_info else None

        df["file_owner"] = df["file"].map(_get_owner_name_safe)

        # add extension (something like the language)
        logger.debug("Extracting file extensions...")
        df["ext"] = df["file"].map(lambda x: x.split(".")[-1] if "." in x else "")  # Handle files without extensions

        # add in last edit date for the file
        logger.debug("Mapping last edit dates...")
        df["last_edit_date"] = df["file"].map(lambda x: self._get_last_edit_date(x, rev=rev))

        # Add repository labels without setting index
        df = self._add_labels_to_df(df)

        logger.info(f"Finished fetching file details for rev '{rev}'. Found details for {len(df)} files.")
        return df

    def time_between_revs(self, rev1, rev2):
        """Calculates the time difference in days between two revisions.

        Args:
            rev1 (str): The first revision (commit hash or tag).
            rev2 (str): The second revision (commit hash or tag).

        Returns:
            float: The absolute time difference in days between the two revisions.

        Note:
            The result is always non-negative (absolute value).
        """
        c1 = self.repo.commit(rev1)
        c2 = self.repo.commit(rev2)
        t1 = pd.to_datetime(c1.committed_date, unit="s", utc=True)
        t2 = pd.to_datetime(c2.committed_date, unit="s", utc=True)
        return abs((t2 - t1).total_seconds()) / (60 * 60 * 24)

    def diff_stats_between_revs(self, rev1, rev2, ignore_globs=None, include_globs=None):
        """Computes diff statistics between two revisions.

        Calculates the total insertions, deletions, net line change, and number of files changed
        between two arbitrary revisions (commits or tags). Optionally filters files using glob patterns.

        Args:
            rev1 (str): The base revision (commit hash or tag).
            rev2 (str): The target revision (commit hash or tag).
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore.
            include_globs (Optional[List[str]]): List of glob patterns for files to include.

        Returns:
            dict: A dictionary with keys:
                - 'insertions' (int): Total lines inserted.
                - 'deletions' (int): Total lines deleted.
                - 'net' (int): Net lines changed (insertions - deletions).
                - 'files_changed' (int): Number of files changed.
                - 'files' (List[str]): List of changed file paths.

        Note:
            Binary files or files that cannot be parsed are skipped.
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """
        diff = self.repo.git.diff(rev1, rev2, "--numstat", "--no-renames")
        insertions = deletions = files_changed = 0
        files = set()
        for line in diff.splitlines():
            parts = line.strip().split("\t")
            if len(parts) == 3:
                ins, dels, fname = parts
                if ins == "-" or dels == "-":
                    continue  # binary or unparseable
                if not self.__check_extension({fname: None}, ignore_globs, include_globs):
                    continue
                insertions += int(ins)
                deletions += int(dels)
                files_changed += 1
                files.add(fname)
        return {
            "insertions": insertions,
            "deletions": deletions,
            "net": insertions - deletions,
            "files_changed": files_changed,
            "files": list(files),
        }

    def committers_between_revs(self, rev1, rev2, ignore_globs=None, include_globs=None):
        """Finds unique committers and authors between two revisions.

        Iterates through all commits between two revisions (exclusive of rev1, inclusive of rev2)
        and returns the unique committers and authors who contributed, filtered by file globs if provided.

        Args:
            rev1 (str): The base revision (commit hash or tag).
            rev2 (str): The target revision (commit hash or tag).
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore.
            include_globs (Optional[List[str]]): List of glob patterns for files to include.

        Returns:
            dict: A dictionary with keys:
                - 'committers' (List[str]): Sorted list of unique committer names.
                - 'authors' (List[str]): Sorted list of unique author names.

        Note:
            Only commits that touch files matching the glob filters are considered.
            The range is interpreted as Git does: rev1..rev2 means commits reachable from rev2 but not rev1.
        """
        commits = list(self.repo.iter_commits(f"{rev1}..{rev2}"))
        committers = set()
        authors = set()
        for c in commits:
            # Check if any file in commit matches globs
            files = self.__check_extension(c.stats.files, ignore_globs, include_globs)
            if not files:
                continue
            if hasattr(c.committer, "name"):
                committers.add(c.committer.name)
            if hasattr(c.author, "name"):
                authors.add(c.author.name)
        return {"committers": sorted(committers), "authors": sorted(authors)}

    def files_changed_between_revs(self, rev1, rev2, ignore_globs=None, include_globs=None):
        """Lists files changed between two revisions.

        Returns a sorted list of all files changed between two arbitrary revisions (commits or tags),
        optionally filtered by glob patterns.

        Args:
            rev1 (str): The base revision (commit hash or tag).
            rev2 (str): The target revision (commit hash or tag).
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore.
            include_globs (Optional[List[str]]): List of glob patterns for files to include.

        Returns:
            List[str]: Sorted list of file paths changed between the two revisions.

        Note:
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """
        diff = self.repo.git.diff(rev1, rev2, "--name-only", "--no-renames")
        files = set()
        for fname in diff.splitlines():
            if not fname.strip():
                continue
            if not self.__check_extension({fname: None}, ignore_globs, include_globs):
                continue
            files.add(fname)
        return sorted(files)

    @multicache(key_prefix="release_tag_summary", key_list=["tag_glob", "include_globs", "ignore_globs"])
    def release_tag_summary(self, tag_glob=None, ignore_globs=None, include_globs=None):
        """Summarizes repository activity between release tags.

        For each tag (filtered by glob), computes the time since the previous tag, diff statistics,
        committers/authors involved, and files changed between tags. Returns a DataFrame with one row
        per tag and columns for all computed metrics.

        Args:
            tag_glob (Optional[Union[str, List[str]]]): Glob pattern(s) to filter tags (e.g., 'v*' or
                ['v*', 'release-*']). If None, all tags are included.
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore in diff/commit analysis.
            include_globs (Optional[List[str]]): List of glob patterns for files to include in diff/commit analysis.

        Returns:
            pandas.DataFrame: DataFrame with columns:
                - tag (str): Tag name
                - tag_date (datetime): Tag creation date
                - commit_sha (str): SHA of the tagged commit
                - time_since_prev (float): Days since previous tag
                - insertions (int): Lines inserted since previous tag
                - deletions (int): Lines deleted since previous tag
                - net (int): Net lines changed since previous tag
                - files_changed (int): Number of files changed since previous tag
                - committers (List[str]): Committers between previous and current tag
                - authors (List[str]): Authors between previous and current tag
                - files (List[str]): Files changed between previous and current tag

        Note:
            The first tag in the sorted list will have NaN for time_since_prev and empty diff/commit info.
            Tag filtering uses fnmatch and supports multiple globs.
        """
        tags_df = self.tags().reset_index()
        if tags_df.empty:
            return pd.DataFrame(
                columns=[
                    "tag",
                    "tag_date",
                    "commit_sha",
                    "time_since_prev",
                    "insertions",
                    "deletions",
                    "net",
                    "files_changed",
                    "committers",
                    "authors",
                    "files",
                ]
            )

        # Filter tags by glob
        if tag_glob is not None:
            if isinstance(tag_glob, str):
                tag_glob = [tag_glob]
            tags_df = tags_df[tags_df["tag"].apply(lambda t: any(fnmatch.fnmatch(t, g) for g in tag_glob))]
        if tags_df.empty:
            return pd.DataFrame(
                columns=[
                    "tag",
                    "tag_date",
                    "commit_sha",
                    "time_since_prev",
                    "insertions",
                    "deletions",
                    "net",
                    "files_changed",
                    "committers",
                    "authors",
                    "files",
                ]
            )

        # Sort by tag_date ascending
        tags_df = tags_df.sort_values("tag_date").reset_index(drop=True)

        rows = []
        prev_sha = None
        for _idx, row in tags_df.iterrows():
            tag = row["tag"]
            tag_date = row["tag_date"]
            commit_sha = row["commit_sha"]
            if prev_sha is not None:
                time_since_prev = self.time_between_revs(prev_sha, commit_sha)
                diff_stats = self.diff_stats_between_revs(prev_sha, commit_sha, ignore_globs, include_globs)
                commit_info = self.committers_between_revs(prev_sha, commit_sha, ignore_globs, include_globs)
                files = self.files_changed_between_revs(prev_sha, commit_sha, ignore_globs, include_globs)
            else:
                time_since_prev = float("nan")
                diff_stats = {"insertions": 0, "deletions": 0, "net": 0, "files_changed": 0, "files": []}
                commit_info = {"committers": [], "authors": []}
                files = []
            rows.append(
                {
                    "tag": tag,
                    "tag_date": tag_date,
                    "commit_sha": commit_sha,
                    "time_since_prev": time_since_prev,
                    "insertions": diff_stats["insertions"],
                    "deletions": diff_stats["deletions"],
                    "net": diff_stats["net"],
                    "files_changed": diff_stats["files_changed"],
                    "committers": commit_info["committers"],
                    "authors": commit_info["authors"],
                    "files": files,
                }
            )
            prev_sha = commit_sha
        return pd.DataFrame(rows)

    def safe_fetch_remote(self, remote_name="origin", prune=False, dry_run=False):
        """Safely fetch changes from remote repository.

        Fetches the latest changes from a remote repository without modifying the working directory.
        This is a read-only operation that only updates remote-tracking branches.

        Args:
            remote_name (str, optional): Name of remote to fetch from. Defaults to 'origin'.
            prune (bool, optional): Remove remote-tracking branches that no longer exist on remote.
                Defaults to False.
            dry_run (bool, optional): Show what would be fetched without actually fetching.
                Defaults to False.

        Returns:
            dict: Fetch results with keys:
                - success (bool): Whether the fetch was successful
                - message (str): Status message or error description
                - remote_exists (bool): Whether the specified remote exists
                - changes_available (bool): Whether new changes were fetched
                - error (Optional[str]): Error message if fetch failed

        Note:
            This method is safe as it only fetches remote changes and never modifies
            the working directory or current branch. It will not perform any merges,
            rebases, or checkouts.
        """
        logger.info(f"Attempting to safely fetch from remote '{remote_name}' (dry_run={dry_run})")

        result = {"success": False, "message": "", "remote_exists": False, "changes_available": False, "error": None}

        try:
            # Check if we have any remotes
            if not self.repo.remotes:
                result["message"] = "No remotes configured for this repository"
                logger.warning(f"No remotes configured for repository '{self.repo_name}'")
                return result

            # Check if the specified remote exists
            remote_names = [remote.name for remote in self.repo.remotes]
            if remote_name not in remote_names:
                result["message"] = f"Remote '{remote_name}' not found. Available remotes: {remote_names}"
                logger.warning(f"Remote '{remote_name}' not found in repository '{self.repo_name}'")
                return result

            result["remote_exists"] = True
            remote = self.repo.remote(remote_name)

            # Perform dry run if requested
            if dry_run:
                try:
                    # Get remote refs to see what's available
                    remote_refs = list(remote.refs)
                    result["message"] = f"Dry run: Would fetch from {remote.url}. Remote has {len(remote_refs)} refs."
                    result["success"] = True
                    logger.info(f"Dry run completed for remote '{remote_name}' in repository '{self.repo_name}'")
                    return result
                except Exception as e:
                    result["error"] = f"Dry run failed: {str(e)}"
                    logger.error(f"Dry run failed for remote '{remote_name}' in repository '{self.repo_name}': {e}")
                    return result

            # Perform the actual fetch
            try:
                logger.info(f"Fetching from remote '{remote_name}' in repository '{self.repo_name}'")
                fetch_info = remote.fetch(prune=prune)

                # Check if any changes were fetched
                changes_available = len(fetch_info) > 0
                result["changes_available"] = changes_available

                if changes_available:
                    fetched_refs = [info.ref.name for info in fetch_info if info.ref]
                    result["message"] = f"Successfully fetched {len(fetch_info)} updates. Updated refs: {fetched_refs}"
                    logger.info(
                        f"Fetch completed with {len(fetch_info)} updates from '{remote_name}' "
                        f"in repository '{self.repo_name}'"
                    )
                else:
                    result["message"] = f"Fetch completed - repository is up to date with '{remote_name}'"
                    logger.info(f"Repository '{self.repo_name}' is up to date with remote '{remote_name}'")

                result["success"] = True

            except Exception as e:
                result["error"] = f"Fetch failed: {str(e)}"
                logger.error(f"Fetch failed for remote '{remote_name}' in repository '{self.repo_name}': {e}")

        except Exception as e:
            result["error"] = f"Unexpected error: {str(e)}"
            logger.error(
                f"Unexpected error during fetch from remote '{remote_name}' in repository '{self.repo_name}': {e}"
            )

        return result

    def warm_cache(self, methods=None, **kwargs):
        """Pre-populate cache with commonly used data.

        Executes a set of commonly used repository analysis methods to populate the cache,
        improving performance for subsequent calls. Only methods that support caching
        will be executed.

        Args:
            methods (Optional[List[str]]): List of method names to pre-warm. If None,
                uses a default set of commonly used methods. Available methods:
                - 'commit_history': Load commit history
                - 'branches': Load branch information
                - 'tags': Load tag information
                - 'blame': Load blame information
                - 'file_detail': Load file details
                - 'list_files': Load file listing
                - 'file_change_rates': Load file change statistics
            **kwargs: Additional keyword arguments to pass to the methods.
                Common arguments include:
                - branch: Branch to analyze (default: repository's default branch)
                - limit: Limit number of commits to analyze
                - ignore_globs: List of glob patterns to ignore
                - include_globs: List of glob patterns to include

        Returns:
            dict: Results of cache warming operations with keys:
                - success (bool): Whether cache warming was successful
                - methods_executed (List[str]): List of methods that were executed
                - methods_failed (List[str]): List of methods that failed
                - cache_entries_created (int): Number of cache entries created
                - execution_time (float): Total execution time in seconds
                - errors (List[str]): List of error messages for failed methods

        Note:
            This method will only execute methods if a cache backend is configured.
            If no cache backend is available, it will return immediately with a
            success status but no methods executed.
        """
        logger.info(f"Starting cache warming for repository '{self.repo_name}'")

        result = {
            "success": False,
            "methods_executed": [],
            "methods_failed": [],
            "cache_entries_created": 0,
            "execution_time": 0.0,
            "errors": [],
        }

        import time

        start_time = time.time()

        # Check if caching is enabled
        if self.cache_backend is None:
            result["success"] = True
            result["execution_time"] = time.time() - start_time
            logger.info(f"No cache backend configured for repository '{self.repo_name}' - skipping cache warming")
            return result

        # Default methods to warm if none specified
        if methods is None:
            methods = ["commit_history", "branches", "tags", "blame", "file_detail", "list_files"]

        # Get initial cache size
        initial_cache_size = len(self.cache_backend._cache) if hasattr(self.cache_backend, "_cache") else 0

        # Execute each method to warm the cache
        for method_name in methods:
            try:
                if not hasattr(self, method_name):
                    result["methods_failed"].append(method_name)
                    result["errors"].append(f"Method '{method_name}' not found")
                    logger.warning(f"Method '{method_name}' not found in repository '{self.repo_name}'")
                    continue

                method = getattr(self, method_name)

                # Execute method with provided kwargs
                logger.debug(f"Executing method '{method_name}' for cache warming in repository '{self.repo_name}'")

                # Handle special cases for method arguments
                method_kwargs = kwargs.copy()

                # For methods that might need specific arguments
                if method_name in ["commit_history", "file_change_rates"]:
                    # Set reasonable defaults if not provided
                    if "limit" not in method_kwargs:
                        method_kwargs["limit"] = 100  # Reasonable default for cache warming
                elif method_name == "list_files":
                    # list_files doesn't accept limit parameter, remove it if present
                    method_kwargs.pop("limit", None)

                # Execute the method
                _ = method(**method_kwargs)
                result["methods_executed"].append(method_name)
                logger.debug(
                    f"Successfully executed method '{method_name}' for cache warming in repository '{self.repo_name}'"
                )

            except Exception as e:
                result["methods_failed"].append(method_name)
                error_msg = f"Method '{method_name}' failed: {str(e)}"
                result["errors"].append(error_msg)
                logger.error(f"Cache warming failed for method '{method_name}' in repository '{self.repo_name}': {e}")

        # Calculate cache entries created
        final_cache_size = len(self.cache_backend._cache) if hasattr(self.cache_backend, "_cache") else 0
        result["cache_entries_created"] = final_cache_size - initial_cache_size

        # Calculate execution time
        result["execution_time"] = time.time() - start_time

        # Determine overall success
        result["success"] = len(result["methods_executed"]) > 0

        if result["success"]:
            logger.info(
                f"Cache warming completed for repository '{self.repo_name}'. "
                f"Executed {len(result['methods_executed'])} methods, "
                f"created {result['cache_entries_created']} cache entries "
                f"in {result['execution_time']:.2f} seconds"
            )
        else:
            logger.warning(
                f"Cache warming failed for repository '{self.repo_name}'. "
                f"No methods executed successfully. Errors: {result['errors']}"
            )

        return result

    def invalidate_cache(self, keys=None, pattern=None):
        """Invalidate specific cache entries or all cache entries for this repository.

        Args:
            keys (Optional[List[str]]): List of specific cache keys to invalidate
            pattern (Optional[str]): Pattern to match cache keys (supports * wildcard)

        Returns:
            int: Number of cache entries invalidated

        Note:
            If both keys and pattern are None, all cache entries for this repository are invalidated.
            Cache keys are automatically prefixed with repository name.
        """
        if self.cache_backend is None:
            logger.warning(f"No cache backend configured for repository '{self.repo_name}' - cannot invalidate cache")
            return 0

        if not hasattr(self.cache_backend, "invalidate_cache"):
            logger.warning(f"Cache backend {type(self.cache_backend).__name__} does not support cache invalidation")
            return 0

        # If specific keys provided, prefix them with repo name
        prefixed_keys = None
        if keys:
            prefixed_keys = [f"*||{self.repo_name}||*{key}*" if not key.startswith("*") else key for key in keys]

        # If pattern provided, include repo name in pattern
        repo_pattern = None
        if pattern:
            repo_pattern = f"*||{self.repo_name}||*{pattern}*"
        elif keys is None:
            # No keys or pattern specified, invalidate all for this repo
            repo_pattern = f"*||{self.repo_name}||*"

        try:
            if prefixed_keys and repo_pattern:
                # Both keys and pattern specified
                count1 = self.cache_backend.invalidate_cache(pattern=repo_pattern)
                count2 = self.cache_backend.invalidate_cache(pattern=prefixed_keys[0] if prefixed_keys else None)
                return count1 + count2
            elif prefixed_keys:
                # Only keys specified
                return sum(self.cache_backend.invalidate_cache(pattern=key) for key in prefixed_keys)
            else:
                # Only pattern (or neither, defaulting to repo pattern)
                return self.cache_backend.invalidate_cache(pattern=repo_pattern)
        except Exception as e:
            logger.error(f"Error invalidating cache for repository '{self.repo_name}': {e}")
            return 0

    def get_cache_stats(self):
        """Get cache statistics for this repository.

        Returns:
            dict: Cache statistics including repository-specific and global cache information
        """
        if self.cache_backend is None:
            return {
                "repository": self.repo_name,
                "cache_backend": None,
                "repository_entries": 0,
                "global_cache_stats": None,
            }

        # Get global cache stats
        global_stats = None
        if hasattr(self.cache_backend, "get_cache_stats"):
            try:
                global_stats = self.cache_backend.get_cache_stats()
            except Exception as e:
                logger.error(f"Error getting global cache stats: {e}")

        # Count repository-specific entries
        repo_entries = 0
        if hasattr(self.cache_backend, "list_cached_keys"):
            try:
                all_keys = self.cache_backend.list_cached_keys()
                repo_entries = len([key for key in all_keys if self.repo_name in str(key.get("key", ""))])
            except Exception as e:
                logger.error(f"Error counting repository cache entries: {e}")

        return {
            "repository": self.repo_name,
            "cache_backend": type(self.cache_backend).__name__,
            "repository_entries": repo_entries,
            "global_cache_stats": global_stats,
        }


class GitFlowRepository(Repository):
    """
    A special case where git flow is followed, so we know something about the branching scheme
    """

    def __init__(self):
        super().__init__()
