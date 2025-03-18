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
    return repo.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints)


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
    ):
        if working_dir is None:
            self.repo_dirs = {x[0].split(".git")[0] for x in os.walk(os.getcwd()) if ".git" in x[0]}
        elif isinstance(working_dir, list):
            self.repo_dirs = working_dir
        else:
            self.repo_dirs = {x[0].split(".git")[0] for x in os.walk(working_dir) if ".git" in x[0]}

        if all(isinstance(r, Repository) for r in self.repo_dirs):
            self.repos = self.repo_dirs
        else:
            self.repos = [
                Repository(r, verbose=verbose, tmp_dir=tmp_dir, cache_backend=cache_backend) for r in self.repo_dirs
            ]

        if ignore_repos is not None:
            self.repos = [x for x in self.repos if x.repo_name not in ignore_repos]

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

        ds = [[x.repo_name] for x in self.repos]
        df = pd.DataFrame(ds, columns=["repository"])
        return df

    def is_bare(self):
        """
        Returns a dataframe of repo names and whether or not they are bare.

        :return: DataFrame
        """

        ds = [[x.repo_name, x.is_bare()] for x in self.repos]
        df = pd.DataFrame(ds, columns=["repository", "is_bare"])
        return df

    def has_coverage(self):
        """
        Returns a DataFrame of repo names and whether or not they have a .coverage file that can be parsed

        :return: DataFrame
        """

        ds = [[x.repo_name, x.has_coverage()] for x in self.repos]
        df = pd.DataFrame(ds, columns=["repository", "has_coverage"])
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
                cov["repository"] = repo.repo_name
                if not cov.empty:
                    df = pd.concat([df, cov])
            except GitCommandError:
                print(f"Warning! Repo: {repo} seems to not have coverage")

        df = df.reset_index(drop=True)
        return df

    def file_change_rates(
        self,
        branch="master",
        limit=None,
        coverage=False,
        days=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """Analyzes the rate of changes to files across all repositories.

        This method helps identify files with high change rates or unusual edit patterns.
        When combined with test coverage data, it can help identify files that may need
        additional test coverage based on their change frequency.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            limit (Optional[int]): Maximum number of commits to analyze per repository
            coverage (bool, optional): Whether to include test coverage data. Defaults to False.
            days (Optional[int]): If provided, only analyze commits from the last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Repository name
                - unique_committers (int): Number of different committers
                - abs_rate_of_change (float): Average number of lines changed per day
                - net_rate_of_change (float): Average net lines changed per day
                - net_change (int): Total net lines changed
                - abs_change (int): Total absolute lines changed
                - edit_rate (float): Ratio of changes to final size
                If coverage=True, additional columns:
                    - lines_covered (int): Number of lines covered by tests
                    - total_lines (int): Total number of lines
                    - coverage (float): Coverage percentage

        Note:
            Files with high change rates but low test coverage may be good candidates
            for additional testing.
        """

        columns = [
            "repository",
            "unique_committers",
            "abs_rate_of_change",
            "net_rate_of_change",
            "net_change",
            "abs_change",
            "edit_rate",
        ]
        if coverage:
            columns += ["lines_covered", "total_lines", "coverage"]

        # Initialize empty DataFrame with all required columns
        df = pd.DataFrame(columns=columns)

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
                    fcr["repository"] = repo.repo_name
                    df = pd.concat([df, fcr], sort=True)
            except GitCommandError:
                print(f"Warning! Repo: {repo} seems to not have the branch: {branch}")

        # Ensure consistent column order and reset index
        df = df[columns]
        df = df.reset_index(drop=True)
        return df

    def hours_estimate(
        self,
        branch="master",
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
        inspired by: https://github.com/kimmobrunfeldt/git-hours/blob/8aaeee237cb9d9028e7a2592a25ad8468b1f45e4/index.js#L114-L143

        Iterates through the commit history of repo to estimate the time commitement of each author or committer over
        the course of time indicated by limit/extensions/days/etc.

        :param branch: the branch to return commits for
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

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        com = "committer" if committer else "author"

        df = pd.DataFrame(columns=[com, "hours", "repository"])

        for repo in self.repos:
            try:
                ch = repo.hours_estimate(
                    branch,
                    grouping_window=grouping_window,
                    single_commit_hours=single_commit_hours,
                    limit=limit,
                    days=days,
                    committer=committer,
                    ignore_globs=ignore_globs,
                    include_globs=include_globs,
                )
                ch["repository"] = repo.repo_name
                df = pd.concat([df, ch])
            except GitCommandError:
                print(f"Warning! Repo: {repo} seems to not have the branch: {branch}")

        df.reset_index()

        if by == "committer" or by == "author":
            df = df.groupby(com).agg({"hours": sum})
            df = df.reset_index()
        elif by == "repository":
            df = df.groupby("repository").agg({"hours": sum})
            df = df.reset_index()

        return df

    def commit_history(self, branch, limit=None, days=None, ignore_globs=None, include_globs=None):
        """Returns a DataFrame containing the commit history for all repositories.

        For each repository, retrieves the commit history on the specified branch. Results from all
        repositories are combined into a single DataFrame. If a limit is provided, it is divided by
        the number of repositories to determine how many commits to fetch from each.

        Args:
            branch (str): The branch to analyze (e.g. 'master', 'main')
            limit (Optional[int]): Maximum number of total commits to return. If provided, divided among repos.
            days (Optional[int]): If provided, only return commits from the last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Name of the repository
                - date (datetime, index): Timestamp of the commit
                - author (str): Name of the commit author
                - committer (str): Name of the committer
                - message (str): Commit message
                - lines (int): Total lines changed
                - insertions (int): Lines added
                - deletions (int): Lines removed
                - net (int): Net lines changed (insertions - deletions)

        Note:
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        # Initialize empty DataFrame with all required columns
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
                    df = pd.concat([df, ch], sort=True)
            except GitCommandError:
                print(f"Warning! Repo: {repo} seems to not have the branch: {branch}")

        # Ensure consistent column order and reset index
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
        df = df.reset_index(drop=True)
        return df

    def file_change_history(
        self,
        branch="master",
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """Returns detailed history of all file changes across repositories.

        Unlike commit_history which returns one row per commit, this method returns
        one row per file change, as a single commit may modify multiple files.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            limit (Optional[int]): Maximum number of commits to analyze per repository
            days (Optional[int]): If provided, only analyze commits from the last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Repository name
                - date (datetime): Timestamp of the change
                - author (str): Name of the author
                - committer (str): Name of the committer
                - message (str): Commit message
                - rev (str): Commit hash
                - filename (str): Path of the changed file
                - insertions (int): Lines added
                - deletions (int): Lines removed

        Note:
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        # Initialize empty DataFrame with all required columns
        df = pd.DataFrame(
            columns=[
                "repository",
                "date",
                "author",
                "committer",
                "message",
                "rev",
                "filename",
                "insertions",
                "deletions",
            ]
        )

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
                    ch["repository"] = repo.repo_name
                    df = pd.concat([df, ch], sort=True)
            except GitCommandError:
                print(f"Warning! Repo: {repo} seems to not have the branch: {branch}")

        # Ensure consistent column order and reset index
        df = df[
            [
                "repository",
                "date",
                "author",
                "committer",
                "message",
                "rev",
                "filename",
                "insertions",
                "deletions",
            ]
        ]
        df = df.reset_index(drop=True)
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
                        df = pd.concat([df, blame_df])
            except GitCommandError:
                print(f"Warning! Repo: {repo} couldnt be blamed")
                pass

        # Reset all index levels
        df = df.reset_index()

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
                    chunk["repository"] = repo.repo_name
                    if not chunk.empty:
                        df = pd.concat([df, chunk])
            except GitCommandError:
                print(f"Warning! Repo: {repo} couldnt be inspected")

        df = df.reset_index()
        df = df.set_index(["file", "repository"])
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

        df = pd.DataFrame(columns=["repository", "local", "branch"])

        if _has_joblib:
            ds = Parallel(n_jobs=-1, backend="threading", verbose=0)(delayed(_branches_func)(x) for x in self.repos)
            for d in ds:
                if not d.empty:
                    df = pd.concat([df, d])
        else:
            for repo in self.repos:
                try:
                    branches_df = _branches_func(repo)
                    if not branches_df.empty:
                        df = pd.concat([df, branches_df])
                except GitCommandError:
                    print(f"Warning! Repo: {repo} couldn't be inspected")

        df = df.reset_index(drop=True)
        return df

    def revs(self, branch="master", limit=None, skip=None, num_datapoints=None):
        """Returns revision information for each repository.

        Retrieves revision (commit) information from each repository, with options
        for limiting or sampling the revisions returned.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            limit (Optional[int]): Maximum number of revisions per repository
            skip (Optional[int]): If provided, only return every Nth revision
            num_datapoints (Optional[int]): If provided, evenly sample this many
                revisions from each repository's history

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Repository name
                - rev (str): Commit hash
                - date (datetime): Timestamp of the revision

        Note:
            If num_datapoints is provided, it overrides limit and skip parameters.
            The sampling will attempt to space the revisions evenly across the
            repository's history.
        """

        if limit is not None:
            limit = math.floor(float(limit) / len(self.repos))

        if num_datapoints is not None:
            num_datapoints = math.floor(float(num_datapoints) / len(self.repos))

        df = pd.DataFrame(columns=["repository", "rev"])

        if _has_joblib:
            ds = Parallel(n_jobs=-1, backend="threading", verbose=0)(
                delayed(_revs_func)(x, branch, limit, skip, num_datapoints) for x in self.repos
            )
            for d in ds:
                if not d.empty:
                    df = pd.concat([df, d])
        else:
            for repo in self.repos:
                try:
                    revs = repo.revs(
                        branch=branch,
                        limit=limit,
                        skip=skip,
                        num_datapoints=num_datapoints,
                    )
                    revs["repository"] = repo.repo_name
                    if not revs.empty:
                        df = pd.concat([df, revs])
                except GitCommandError:
                    print(f"Warning! Repo: {repo} couldn't be inspected")

        df = df.reset_index(drop=True)
        return df

    def cumulative_blame(
        self,
        branch="master",
        by="committer",
        limit=None,
        skip=None,
        num_datapoints=None,
        committer=True,
        ignore_globs=None,
        include_globs=None,
    ):
        """Analyzes how code ownership has evolved over time.

        For each revision point, calculates the lines of code attributed to each
        contributor, showing how ownership of the codebase has changed over time.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            by (str, optional): How to group the results. One of:
                - 'committer': One column per committer (default)
                - 'project': One column per project
                - 'raw': One column per committer per project
            limit (Optional[int]): Maximum number of revisions to analyze
            skip (Optional[int]): If provided, only analyze every Nth revision
            num_datapoints (Optional[int]): If provided, evenly sample this many points
            committer (bool, optional): If True, use committer info. If False, use author.
                Defaults to True.
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame indexed by date with columns depending on 'by':
                If by='committer':
                    One column per committer showing their LOC over time
                If by='project':
                    One column per project showing total LOC over time
                If by='raw':
                    One column per committer-project combination

        Note:
            Missing values are forward-filled, then filled with 0.
            This assumes that if a contributor hasn't modified their code, they
            maintain the same LOC count from their last contribution.
        """

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
                print(f"Warning! Repo: {repo} couldn't be inspected")
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
        dfs = []
        for repo in self.repos:
            try:
                dfs.append(repo.commits_in_tags(**kwargs))
            except GitCommandError as e:
                print(f"Warning! Repo: {repo} couldn't be inspected because of {e!r}")
        df = pd.concat(dfs)
        return df

    def tags(self):
        """
        Returns a data frame of all tags in origin.  The DataFrame will have the columns:

         * repository
         * tag

        :returns: DataFrame
        """

        if _has_joblib:
            dfs = Parallel(n_jobs=-1, backend="threading", verbose=0)(delayed(_tags_func)(x) for x in self.repos)
        else:
            dfs = []
            for repo in self.repos:
                try:
                    dfs.append(repo.tags())
                except GitCommandError:
                    print(f"Warning! Repo: {repo} couldn't be inspected")
        # Filter out empty DataFrames before concatenation
        dfs = [df for df in dfs if not df.empty]
        df = pd.concat(dfs) if dfs else pd.DataFrame()
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
                - 'file': Not implemented yet

        Returns:
            pandas.DataFrame: A DataFrame with columns depending on the 'by' parameter:
                If by='projectd':
                    - projectd (str): Always 'projectd'
                    - bus factor (int): Bus factor for entire project
                If by='repository':
                    - repository (str): Repository name
                    - bus factor (int): Bus factor for that repository

        Raises:
            NotImplementedError: If by='file' is specified (not implemented yet)

        Note:
            A low bus factor (e.g. 1-2) indicates high risk as knowledge is concentrated among
            few contributors. A higher bus factor indicates knowledge is better distributed.
        """

        if by == "file":
            raise NotImplementedError("File-wise bus factor")
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

            return pd.DataFrame([["projectd", tc]], columns=["projectd", "bus factor"])
        elif by == "repository":
            df = pd.DataFrame(columns=["repository", "bus factor"])
            for repo in self.repos:
                try:
                    bf_df = repo.bus_factor(ignore_globs=include_globs, include_globs=include_globs, by=by)
                    if not bf_df.empty:
                        df = pd.concat([df, bf_df])
                except GitCommandError:
                    print(f"Warning! Repo: {repo} couldn't be inspected")

            df.reset_index()
            return df

    def punchcard(
        self,
        branch="master",
        limit=None,
        days=None,
        by=None,
        normalize=None,
        ignore_globs=None,
        include_globs=None,
    ):
        """Generates a "punch card" visualization of commit activity.

        Creates a visualization of when commits occur, aggregated by day of week
        and hour of day. This helps identify patterns in development activity.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            limit (Optional[int]): Maximum number of commits to analyze
            days (Optional[int]): If provided, only analyze commits from the last N days
            by (Optional[str]): Additional field to group by (e.g. 'committer')
            normalize (Optional[int]): If provided, normalize counts to this value
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - day (int): Day of week (0=Monday, 6=Sunday)
                - hour (int): Hour of day (0-23)
                - lines (int/float): Lines changed
                - insertions (int/float): Lines added
                - deletions (int/float): Lines removed
                - net (int/float): Net lines changed
                If by is specified, includes that column as well.

        Note:
            If normalize is provided, all numeric columns are normalized so their
            sum equals the specified value.
        """

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
                chunk["repository"] = repo.repo_name
                if not chunk.empty:
                    df = pd.concat([df, chunk])
            except GitCommandError:
                print(f"Warning! Repo: {repo} couldn't be inspected")

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

        return punch_card

    def __del__(self):
        """Cleanup method called when the object is destroyed.

        Ensures proper cleanup of all repository objects, including
        temporary directories for cloned repositories.
        """

        for repo in self.repos:
            repo.__del__()


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

        # pull the git urls from github's api
        uri = f"https://api.github.com/users/{username}/repos"
        data = requests.get(uri)
        repos = []
        for chunk in data.json():
            # if we are skipping forks
            if ignore_forks:
                if not chunk["fork"]:
                    repos.append(chunk["git_url"])
            else:
                repos.append(chunk["git_url"])

        ProjectDirectory.__init__(self, working_dir=repos, ignore_repos=ignore_repos, verbose=verbose)
