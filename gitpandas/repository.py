"""
.. module:: repository
   :platform: Unix, Windows
   :synopsis: A module for examining a single git repository

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

import os
import sys
import datetime
import time
import json
import logging
import tempfile
import fnmatch
import shutil
import warnings
import numpy as np
import pandas as pd
from git import Repo, GitCommandError
from gitpandas.cache import multicache, EphemeralCache, RedisDFCache
from pandas import DataFrame, to_datetime

try:
    from joblib import delayed, Parallel

    _has_joblib = True
except ImportError as e:
    _has_joblib = False

__author__ = 'willmcginnis'


def _parallel_cumulative_blame_func(self_, x, committer, ignore_globs, include_globs):
    blm = self_.blame(
        rev=x['rev'],
        committer=committer,
        ignore_globs=ignore_globs,
        include_globs=include_globs
    )
    x.update(json.loads(blm.to_json())['loc'])

    return x


class Repository(object):
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

    Attributes:
        verbose (bool): Whether verbose output is enabled
        git_dir (str): Path to the git repository
        repo (git.Repo): GitPython Repo instance
        cache_backend (Optional[object]): Cache backend being used
        _labels_to_add (List[str]): Labels to add to DataFrames
        _git_repo_name (Optional[str]): Repository name for remote repos

    Examples:
        >>> # Create from local repository
        >>> repo = Repository('/path/to/repo')
        
        >>> # Create from remote repository
        >>> repo = Repository('git://github.com/user/repo.git')

    Note:
        When using remote repositories, they will be cloned to temporary directories.
        This can be slow for large repositories.
    """

    def __init__(self, working_dir=None, verbose=False, tmp_dir=None, cache_backend=None, labels_to_add=None):
        self.verbose = verbose
        self.log = logging.getLogger('gitpandas')
        self.__delete_hook = False
        self._git_repo_name = None
        self.cache_backend = cache_backend
        self._labels_to_add = labels_to_add or []
        if working_dir is not None:
            if working_dir.startswith(('git://', 'https://', 'http://')):
                # if a tmp dir is passed, clone into that, otherwise make a temp directory.
                if tmp_dir is None:
                    if self.verbose:
                        print('cloning repository: %s into a temporary location' % (working_dir,))
                    dir_path = tempfile.mkdtemp()
                else:
                    dir_path = tmp_dir

                self.repo = Repo.clone_from(working_dir, dir_path)
                self._git_repo_name = working_dir.split(os.sep)[-1].split('.')[0]
                self.git_dir = dir_path
                self.__delete_hook = True
            else:
                self.git_dir = working_dir
                self.repo = Repo(self.git_dir)
        else:
            self.git_dir = os.getcwd()
            self.repo = Repo(self.git_dir)

        if self.verbose:
            print('Repository [%s] instantiated at directory: %s' % (self._repo_name(), self.git_dir))

    def __del__(self):
        """Cleanup method called when the object is destroyed.

        Cleans up any temporary directories created for cloned repositories.
        """
        if self.__delete_hook:
            if os.path.exists(self.git_dir):
                shutil.rmtree(self.git_dir)

    def is_bare(self):
        """Checks if this is a bare repository.

        A bare repository is one without a working tree, typically used as a central
        repository.

        Returns:
            bool: True if this is a bare repository, False otherwise
        """

        return self.repo.bare

    def has_coverage(self):
        """Checks if a parseable .coverage file exists in the repository.

        Attempts to find and parse a .coverage file in the repository root directory.
        The file must be in a valid format that can be parsed as JSON.

        Returns:
            bool: True if a valid .coverage file exists, False otherwise
        """

        if os.path.exists(self.git_dir + os.sep + '.coverage'):
            try:
                with open(self.git_dir + os.sep + '.coverage', 'r') as f:
                    blob = f.read()
                    blob = blob.split('!')[2]
                    json.loads(blob)
                return True
            except Exception:
                return False
        else:
            return False

    def coverage(self):
        """Analyzes test coverage information from the repository.

        Attempts to parse the .coverage file if it exists and returns coverage
        statistics for each file.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - filename (str): Path to the file
                - lines_covered (int): Number of lines covered by tests
                - total_lines (int): Total number of lines
                - coverage (float): Coverage percentage
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            Returns an empty DataFrame with the correct columns if no coverage
            file exists or it can't be parsed.
        """

        if not self.has_coverage():
            return DataFrame(columns=['filename', 'lines_covered', 'total_lines', 'coverage'])

        with open(self.git_dir + os.sep + '.coverage', 'r') as f:
            blob = f.read()
            blob = blob.split('!')[2]
            cov = json.loads(blob)

        ds = []
        for filename in cov['lines'].keys():
            idx = 0
            try:
                with open(filename, 'r') as f:
                    for idx, _ in enumerate(f):
                        pass
            except FileNotFoundError as e:
                if self.verbose:
                    warnings.warn('Could not find file %s for coverage' % (filename, ))

            num_lines = idx + 1

            try:
                short_filename = filename.split(self.git_dir + os.sep)[1]
                ds.append([short_filename, len(cov['lines'][filename]), num_lines])
            except IndexError as e:
                if self.verbose:
                    warnings.warn('Could not find file %s for coverage' % (filename, ))

        df = DataFrame(ds, columns=['filename', 'lines_covered', 'total_lines'])
        df['coverage'] = df['lines_covered'] / df['total_lines']
        df = self._add_labels_to_df(df)

        return df

    def hours_estimate(self, branch='master', grouping_window=0.5, single_commit_hours=0.5, limit=None, days=None,
                       committer=True, ignore_globs=None, include_globs=None):
        """
        inspired by: https://github.com/kimmobrunfeldt/git-hours/blob/8aaeee237cb9d9028e7a2592a25ad8468b1f45e4/index.js#L114-L143

        Iterates through the commit history of repo to estimate the time commitement of each author or committer over
        the course of time indicated by limit/extensions/days/etc.

        :param branch: the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param grouping_window: (optional, default=0.5 hours) the threhold for how close two commits need to be to consider them part of one coding session
        :param single_commit_hours: (optional, default 0.5 hours) the time range to associate with one single commit
        :param days: (optional, default=None) number of days to return, if limit is None
        :param committer: (optional, default=True) whether to use committer vs. author
        :param ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
        :param include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.
        :return: DataFrame
        """

        max_diff_in_minutes = grouping_window * 60.0
        first_commit_addition_in_minutes = single_commit_hours * 60.0

        # First get the commit history
        ch = self.commit_history(branch=branch, limit=limit, days=days, ignore_globs=ignore_globs,
                                 include_globs=include_globs)

        # split by committer|author
        if committer:
            by = 'committer'
        else:
            by = 'author'
        people = set(ch[by].values)

        ds = []
        for person in people:
            commits = ch[ch[by] == person]
            commits_ts = [x * 10e-10 for x in sorted(commits.index.values.tolist())]

            if len(commits_ts) < 2:
                ds.append([person, 0])
                continue

            def estimate(index, date):
                next_ts = commits_ts[index + 1]
                diff_in_minutes = next_ts - date
                diff_in_minutes /= 60.0
                if diff_in_minutes < max_diff_in_minutes:
                    return diff_in_minutes / 60.0
                return first_commit_addition_in_minutes / 60.0

            hours = [estimate(a, b) for a, b in enumerate(commits_ts[:-1])]
            hours = sum(hours)
            ds.append([person, hours])

        df = DataFrame(ds, columns=[by, 'hours'])
        df = self._add_labels_to_df(df)

        return df

    def commit_history(self, branch='master', limit=None, days=None, ignore_globs=None, include_globs=None):
        """Returns a DataFrame containing the commit history for a branch.

        Retrieves the commit history for the specified branch, with options to limit
        the number of commits or time range, and filter which files to include.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            limit (Optional[int]): Maximum number of commits to return
            days (Optional[int]): If provided, only return commits from the last N days
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
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
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """

        # setup the data-set of commits
        if limit is None:
            if days is None:
                ds = [[
                          x.author.name,
                          x.committer.name,
                          x.committed_date,
                          x.message,
                          x.hexsha,
                          self.__check_extension(x.stats.files, ignore_globs=ignore_globs, include_globs=include_globs)
                      ] for x in self.repo.iter_commits(branch, max_count=sys.maxsize)]
            else:
                ds = []
                c_date = time.time()
                commits = self.repo.iter_commits(branch, max_count=sys.maxsize)
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
                        ds.append([
                            x.author.name,
                            x.committer.name,
                            x.committed_date,
                            x.message,
                            x.hexsha,
                            self.__check_extension(x.stats.files, ignore_globs=ignore_globs,
                                                   include_globs=include_globs)
                        ])

        else:
            ds = [[
                      x.author.name,
                      x.committer.name,
                      x.committed_date,
                      x.message,
                      x.hexsha,
                      self.__check_extension(x.stats.files, ignore_globs=ignore_globs, include_globs=include_globs)
                  ] for x in self.repo.iter_commits(branch, max_count=limit)]

        # aggregate stats
        ds = [x[:-1] + [sum([x[-1][key]['lines'] for key in x[-1].keys()]),
                        sum([x[-1][key]['insertions'] for key in x[-1].keys()]),
                        sum([x[-1][key]['deletions'] for key in x[-1].keys()]),
                        sum([x[-1][key]['insertions'] for key in x[-1].keys()]) - sum(
                            [x[-1][key]['deletions'] for key in x[-1].keys()])
                        ] for x in ds if len(x[-1].keys()) > 0]

        # make it a pandas dataframe
        df = DataFrame(ds,
                       columns=['author', 'committer', 'date', 'message', 'commit_sha', 'lines', 'insertions', 'deletions', 'net'])

        # format the date col and make it the index
        df['date'] = pd.to_datetime(df['date'], unit="s", utc=True)
        df = df.set_index('date')

        df['branch'] = branch
        df = self._add_labels_to_df(df)

        return df

    def file_change_history(self, branch='master', limit=None, days=None, ignore_globs=None, include_globs=None):
        """Returns detailed history of all file changes in a branch.

        Unlike commit_history which returns one row per commit, this method returns
        one row per file change, as a single commit may modify multiple files.

        Args:
            branch (str, optional): Branch to analyze. Defaults to 'master'.
            limit (Optional[int]): Maximum number of commits to analyze
            days (Optional[int]): If provided, only analyze commits from the last N days
            ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
            include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - date (datetime, index): Timestamp of the change
                - author (str): Name of the author
                - committer (str): Name of the committer
                - message (str): Commit message
                - rev (str): Commit hash
                - filename (str): Path of the changed file
                - insertions (int): Lines added
                - deletions (int): Lines removed
                - repository (str): Repository name
                Additional columns for any labels specified in labels_to_add

        Note:
            If both ignore_globs and include_globs are provided, files must match an include pattern
            and not match any ignore patterns to be included.
        """

        # setup the dataset of commits
        if limit is None:
            if days is None:
                ds = [[
                          x.author.name,
                          x.committer.name,
                          x.committed_date,
                          x.message,
                          x.name_rev.split()[0],
                          self.__check_extension(x.stats.files, ignore_globs=ignore_globs, include_globs=include_globs)
                      ] for x in self.repo.iter_commits(branch, max_count=sys.maxsize)]
            else:
                ds = []
                c_date = time.time()
                commits = self.repo.iter_commits(branch, max_count=sys.maxsize)
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
                        ds.append([
                            x.author.name,
                            x.committer.name,
                            x.committed_date,
                            x.message,
                            x.name_rev.split()[0],
                            self.__check_extension(x.stats.files, ignore_globs=ignore_globs,
                                                   include_globs=include_globs)
                        ])

        else:
            ds = [[
                      x.author.name,
                      x.committer.name,
                      x.committed_date,
                      x.message,
                      x.name_rev.split()[0],
                      self.__check_extension(x.stats.files, ignore_globs=ignore_globs, include_globs=include_globs)
                  ] for x in self.repo.iter_commits(branch, max_count=limit)]

        ds = [x[:-1] + [fn, x[-1][fn]['insertions'], x[-1][fn]['deletions']] for x in ds for fn in x[-1].keys() if
              len(x[-1].keys()) > 0]

        # make it a pandas dataframe
        df = DataFrame(ds,
                       columns=['author', 'committer', 'date', 'message', 'rev', 'filename', 'insertions', 'deletions'])

        # format the date col and make it the index
        df['date'] = pd.to_datetime(df['date'], unit="s")  # First convert to datetime
        df = df.set_index('date')
        df.index = df.index.tz_localize("UTC")  # Then localize the index
        df = self._add_labels_to_df(df)

        return df

    def file_change_rates(self, branch='master', limit=None, coverage=False, days=None, ignore_globs=None,
                          include_globs=None):
        """
        This function will return a DataFrame containing some basic aggregations of the file change history data, and
        optionally test coverage data from a coverage_data.py .coverage file.  The aim here is to identify files in the
        project which have abnormal edit rates, or the rate of changes without growing the files size.  If a file has
        a high change rate and poor test coverage, then it is a great candidate for writing more tests.

        :param branch: (optional, default=master) the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param coverage: (optional, default=False) a bool for whether or not to attempt to join in coverage data.
        :param days: (optional, default=None) number of days to return if limit is None
        :param ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
        :param include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.
        :return: DataFrame
        """

        fch = self.file_change_history(
            branch=branch,
            limit=limit,
            days=days,
            ignore_globs=ignore_globs,
            include_globs=include_globs
        )
        fch.reset_index(level=0, inplace=True)

        if fch.shape[0] > 0:
            file_history = fch.groupby('filename').agg({
                'insertions': ['sum', 'max', 'mean'],
                'deletions': ['sum', 'max', 'mean'],
                'message': lambda x: ' | '.join([str(y) for y in x]),
                'committer': lambda x: ' | '.join([str(y) for y in x]),
                'author': lambda x: ' | '.join([str(y) for y in x]),
                'date': ['max', 'min']
            })

            # Flatten column names
            file_history.columns = [
                'total_insertions', 'insertions_max', 'mean_insertions',
                'total_deletions', 'deletions_max', 'mean_deletions',
                'messages', 'committers', 'authors',
                'max_date', 'min_date'
            ]

            # Calculate net changes
            file_history['net_change'] = file_history['total_insertions'] - file_history['total_deletions']
            file_history['abs_change'] = file_history['total_insertions'] + file_history['total_deletions']

            # Calculate time deltas
            file_history['delta_time'] = file_history['max_date'] - file_history['min_date']
            file_history['delta_days'] = file_history['delta_time'].dt.total_seconds() / (60 * 60 * 24)

            # Calculate metrics
            file_history['net_rate_of_change'] = file_history['net_change'] / file_history['delta_days']
            file_history['abs_rate_of_change'] = file_history['abs_change'] / file_history['delta_days']
            file_history['edit_rate'] = file_history['abs_rate_of_change'] - file_history['net_rate_of_change']
            file_history['unique_committers'] = file_history['committers'].map(lambda x: len(set(x.split(' | '))))

            # reindex
            file_history = file_history.reindex(
                columns=['unique_committers', 'abs_rate_of_change', 'net_rate_of_change', 'net_change', 'abs_change',
                         'edit_rate'])
            file_history.sort_values(by=['edit_rate'], inplace=True)

            if coverage and self.has_coverage():
                file_history = file_history.merge(self.coverage(), left_index=True, right_on='filename', how='outer')
                file_history.set_index(keys=['filename'], drop=True, inplace=True)
        else:
            file_history = DataFrame(
                columns=['unique_committers', 'abs_rate_of_change', 'net_rate_of_change', 'net_change', 'abs_change',
                         'edit_rate'])

        file_history = self._add_labels_to_df(file_history)

        return file_history

    @staticmethod
    def __check_extension(files, ignore_globs=None, include_globs=None):
        """
        Internal method to filter a list of file changes by extension and ignore_dirs.

        :param files:
        :param ignore_globs: a list of globs to ignore (if none falls back to extensions and ignore_dir)
        :param include_globs: a list of globs to include (if none, includes all).
        :return: dict
        """

        if include_globs is None or include_globs == []:
            include_globs = ['*']

        out = {}
        for key in files.keys():
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

        return out

    @multicache(
        key_prefix='blame',
        key_list=['rev', 'committer', 'by', 'ignore_blobs', 'include_globs'],
        skip_if=lambda x: True if x.get('rev') is None or x.get('rev') == 'HEAD' else False
    )
    def blame(self, rev='HEAD', committer=True, by='repository', ignore_globs=None, include_globs=None):
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

        blames = []
        file_names = [x for x in self.repo.git.log(pretty='format:', name_only=True, diff_filter='A').split('\n') if
                      x.strip() != '']
        for file in self.__check_extension({x: x for x in file_names}, ignore_globs=ignore_globs,
                                           include_globs=include_globs).keys():
            try:
                blame_output = self.repo.blame(rev, str(file).replace(self.git_dir + '/', ''))
                for commit, lines in blame_output:
                    blames.append((commit, lines, str(file).replace(self.git_dir + '/', '')))
            except GitCommandError:
                pass

        if committer:
            if by == 'repository':
                blames_df = DataFrame(
                    [[x[0].committer.name, len(x[1])] for x in blames],
                    columns=['committer', 'loc']
                ).groupby('committer')['loc'].sum().to_frame()
            elif by == 'file':
                blames_df = DataFrame(
                    [[x[0].committer.name, len(x[1]), x[2]] for x in blames],
                    columns=['committer', 'loc', 'file']
                ).groupby(['committer', 'file'])['loc'].sum().to_frame()
        else:
            if by == 'repository':
                blames_df = DataFrame(
                    [[x[0].author.name, len(x[1])] for x in blames],
                    columns=['author', 'loc']
                ).groupby('author')['loc'].sum().to_frame()
            elif by == 'file':
                blames_df = DataFrame(
                    [[x[0].author.name, len(x[1]), x[2]] for x in blames],
                    columns=['author', 'loc', 'file']
                ).groupby(['author', 'file'])['loc'].sum().to_frame()

        blames_df = self._add_labels_to_df(blames_df)

        return blames_df

    def revs(self, branch='master', limit=None, skip=None, num_datapoints=None):
        """
        Returns a dataframe of all revision tags and their timestamps. It will have the columns:

         * date
         * rev

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :param num_datapoints: (optional, default=None) if limit and skip are none, and this isn't, then num_datapoints evenly spaced revs will be used
        :return: DataFrame

        """

        if limit is None and skip is None and num_datapoints is not None:
            limit = sum(1 for _ in self.repo.iter_commits())
            skip = int(float(limit) / num_datapoints)
        else:
            if limit is None:
                limit = sys.maxsize
            elif skip is not None:
                limit = limit * skip

        ds = [[x.committed_date, x.name_rev.split(' ')[0]] for x in self.repo.iter_commits(branch, max_count=limit)]
        df = DataFrame(ds, columns=['date', 'rev'])

        if skip is not None:
            if skip == 0:
                skip = 1

            if df.shape[0] >= skip:
                df = df.iloc[range(0, df.shape[0], skip)]
                df.reset_index(drop=True, inplace=True)
            else:
                df = df.iloc[[0]]
                df.reset_index(drop=True, inplace=True)

        df = self._add_labels_to_df(df)

        return df

    def cumulative_blame(self, branch='master', limit=None, skip=None, num_datapoints=None, committer=True,
                         ignore_globs=None, include_globs=None):
        """
        Returns the blame at every revision of interest. Index is a datetime, column per committer, with number of lines
        blamed to each committer at each timestamp as data.

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :param num_datapoints: (optional, default=None) if limit and skip are none, and this isn't, then num_datapoints evenly spaced revs will be used
        :param committer: (optional, defualt=True) true if committer should be reported, false if author
        :param ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
        :param include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.
        :return: DataFrame

        """

        revs = self.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints)

        # get the commit history to stub out committers (hacky and slow)
        if sys.version_info.major == 2:
            committers = set([x.committer.name for x in self.repo.iter_commits(branch, max_count=sys.maxsize)])
        else:
            committers = {x.committer.name for x in self.repo.iter_commits(branch, max_count=sys.maxsize)}

        for y in committers:
            revs[y] = 0

        if self.verbose:
            print('Beginning processing for cumulative blame:')

        # now populate that table with some actual values
        for idx, row in revs.iterrows():
            if self.verbose:
                print('%s. [%s] getting blame for rev: %s' % (
                str(idx), datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), row.rev,))

            blame = self.blame(rev=row.rev, committer=committer, ignore_globs=ignore_globs, include_globs=include_globs)
            for y in committers:
                try:
                    loc = blame.loc[y, 'loc']
                    revs.at[idx, y] = loc
                except KeyError:
                    pass

        del revs['rev']

        revs['date'] = pd.to_datetime(revs['date'], unit="s", utc=True)
        revs.set_index(keys=['date'], drop=True, inplace=True)
        revs = revs.fillna(0.0)

        # drop 0 cols
        for col in revs.columns.values:
            if col != 'col':
                if revs[col].sum() == 0:
                    del revs[col]

        # drop 0 rows
        keep_idx = []
        committers = [x for x in revs.columns.values if x != 'date']
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

        revs = revs.loc[keep_idx]

        return revs

    def parallel_cumulative_blame(self, branch='master', limit=None, skip=None, num_datapoints=None, committer=True,
                                  workers=1, ignore_globs=None, include_globs=None):
        """
        Returns the blame at every revision of interest. Index is a datetime, column per committer, with number of lines
        blamed to each committer at each timestamp as data.

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :param num_datapoints: (optional, default=None) if limit and skip are none, and this isn't, then num_datapoints evenly spaced revs will be used
        :param committer: (optional, defualt=True) true if committer should be reported, false if author
        :param ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
        :param include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.
        :param workers: (optional, default=1) integer, the number of workers to use in the threadpool, -1 for one per core.
        :return: DataFrame

        """

        if not _has_joblib:
            raise ImportError('''Must have joblib installed to use parallel_cumulative_blame(), please use
            cumulative_blame() instead.''')

        revs = self.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints)

        if self.verbose:
            print('Beginning processing for cumulative blame:')

        revisions = json.loads(revs.to_json(orient='index'))
        revisions = [revisions[key] for key in revisions]

        ds = Parallel(n_jobs=workers, backend='threading', verbose=5)(
            delayed(_parallel_cumulative_blame_func)
            (self, x, committer, ignore_globs, include_globs) for x in revisions
        )

        revs = DataFrame(ds)
        del revs['rev']

        revs['date'] = pd.to_datetime(revs['date'], unit="s", utc=True)
        revs.set_index(keys=['date'], drop=True, inplace=True)
        revs = revs.fillna(0.0)

        # drop 0 cols
        for col in revs.columns.values:
            if col != 'col':
                if revs[col].sum() == 0:
                    del revs[col]

        # drop 0 rows
        keep_idx = []
        committers = [x for x in revs.columns.values if x != 'date']
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

        revs = revs.iloc[keep_idx]
        revs.sort_index(ascending=False, inplace=True)

        return revs

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

        # first pull the local branches
        local_branches = self.repo.branches
        data = [[x.name, True] for x in list(local_branches)]

        # then the remotes
        remote_branches = self.repo.git.branch('-r').replace(" ", "").splitlines()
        rb = []
        for i, remote in enumerate(remote_branches):
            if "->" in remote:
                continue
            # Strip origin/ prefix
            if remote.startswith('origin/'):
                remote = remote[7:]
            rb.append(remote)
        remote_branches = set(rb)

        data += [[x, False] for x in remote_branches]

        df = DataFrame(data, columns=['branch', 'local'])
        df = self._add_labels_to_df(df)

        return df

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
        branches = self.repo.git.branch('-a', '--contains', commit).replace(" ", "").lstrip("*").splitlines()
        df = DataFrame(branches, columns=["branch"])
        df["commit"] = str(commit)
        df = self._add_labels_to_df(df)

        return df

    def commits_in_tags(self, start=np.timedelta64(6, "M"), end=None):
        """Analyzes commits associated with each tag.

        For each tag, traces backwards through the commit history until hitting another
        tag, reaching the time limit, or hitting the root commit. This helps understand
        what changes went into each tagged version.

        Args:
            start (Union[pd.Timestamp, np.timedelta64, pd.Timedelta], optional):
                Start time for commit analysis. If a timedelta is provided, it's
                calculated relative to today. Defaults to 6 months before today.
            end (Optional[Union[pd.Timestamp, np.timedelta64, pd.Timedelta]]):
                End time for commit analysis. If a timedelta is provided, it's
                calculated relative to today. Defaults to None (no end limit).

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

        # If we pass in a timedelta instead of a timestamp, calc the timestamp relative to now
        if isinstance(start, pd.Timedelta) or isinstance(start, np.timedelta64):
            start = pd.Timestamp.today(tz="UTC") - start
        if isinstance(end, pd.Timedelta) or isinstance(end, np.timedelta64):
            end = pd.Timestamp.today(tz="UTC") - end

        # remove tagged commits outside our date ranges
        df_tags = self.tags()
        if start:
            df_tags = df_tags.query(f'commit_date > "{start}"').copy()
        if end:
            df_tags = df_tags.query(f'commit_date < "{end}"').copy()

        # convert to unix time to speed up calculations later
        start = (start - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta('1s') if start else start
        end = (end - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta('1s') if end else end

        ds = []
        checked_commits = set()

        df_tags["filled_shas"] = df_tags["tag_sha"].fillna(value=df_tags["commit_sha"])
        for sha, tag in df_tags[["filled_shas", "tag"]].sort_index(level="tag_date").values:
            commit = self.repo.commit(sha)
            before_start = start and commit.committed_date < start
            passed_end = end and commit.committed_date > end
            already_checked = str(commit) in checked_commits
            if before_start or passed_end or already_checked:
                continue
            tag = self.repo.tag(tag)

            checked_commits.add(str(commit))
            ds.append(self._commits_per_tags_helper(commit, df_tags, tag=tag)[0])

        for sha, tag in df_tags[["filled_shas", "tag"]].sort_index(level="tag_date").values:
            commit = self.repo.commit(sha)
            tag = self.repo.tag(tag)
            self._commits_per_tags_recursive(commit=commit, df_tags=df_tags, ds=ds, start=start, end=end,
                                             checked_commits=checked_commits, tag=tag)
        df = pd.DataFrame(ds, columns=["tag_date", "commit_date", "commit_sha", "tag"])
        df = self._add_labels_to_df(df)

        df = df.sort_values(by=["tag", "commit_date"])
        df = df.set_index(keys=['tag_date', 'commit_date'], drop=True)

        return df

    def _commits_per_tags_recursive(self, commit, df_tags, ds=None, tag=None, checked_commits=None, start=None,
                                    end=None):
        ds = ds if ds is not None else []
        checked_commits = checked_commits if checked_commits is not None else set()

        for commit in commit.parents:
            before_start = start and commit.committed_date < start
            passed_end = end and commit.committed_date > end
            already_checked = str(commit) in checked_commits
            if before_start or passed_end or already_checked:
                continue
            checked_commits.add(str(commit))
            commit_meta, tag = self._commits_per_tags_helper(commit=commit, df_tags=df_tags, tag=tag)
            ds.append(commit_meta)
            self._commits_per_tags_recursive(commit=commit, df_tags=df_tags, ds=ds, tag=tag,
                                             checked_commits=checked_commits, start=start, end=end)

    def _commits_per_tags_helper(self, commit, df_tags, tag=None):
        tag_pd = (df_tags
                  .loc[
                      (df_tags["commit_sha"].str.contains(str(commit)))
                      | (df_tags["tag_sha"].str.contains(str(commit)))
                  ].tag)
        if not tag_pd.empty:
            tag = self.repo.tag(tag_pd[0])
        if tag and tag.tag:
            tag_date = tag.tag.tagged_date
        else:
            tag_date = commit.committed_date
        tag_date = pd.to_datetime(tag_date, unit="s", utc=True)
        commit_date = pd.to_datetime(commit.committed_date, unit="s", utc=True)
        
        return dict(
            commit_sha=str(commit),
            tag=str(tag),
            tag_date=tag_date,
            commit_date=commit_date
        )

    def tags(self):
        """Returns information about all tags in the repository.

        Retrieves detailed information about all tags, including both lightweight
        and annotated tags.

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

        tags = self.repo.tags
        tags_meta = []
        cols = ["tag_date", "commit_date", "tag", "annotated", "annotation", "tag_sha", "commit_sha"]
        for tag in tags:
            d = dict.fromkeys(cols)
            if tag.tag:
                d["tag_date"] = tag.tag.tagged_date
                d["annotated"] = True
                d["annotation"] = tag.tag.message
                d["tag_sha"] = tag.tag.hexsha
            else:
                d["tag_date"] = tag.commit.committed_date
                d["annotated"] = False
                d["annotation"] = ""
                d["tag_sha"] = None
            d["commit_date"] = tag.commit.committed_date
            d["tag"] = tag.name
            d["commit_sha"] = tag.commit.hexsha

            tags_meta.append(d)
        df = DataFrame(tags_meta, columns=cols)

        df['tag_date'] = to_datetime(df['tag_date'], unit="s", utc=True)
        df['commit_date'] = to_datetime(df['commit_date'], unit="s", utc=True)
        df = self._add_labels_to_df(df)

        df = df.set_index(keys=['tag_date', 'commit_date'], drop=True)
        df = df.sort_index(level=["tag_date", "commit_date"])

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
            if reponame.strip() == '':
                return 'unknown_repo'
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
        df['repository'] = self._repo_name()
        for i, label in enumerate(self._labels_to_add):
            df[f"label{i}"] = label
        return df

    def __str__(self):
        """Returns a human-readable string representation of the repository.

        Returns:
            str: String in format 'git repository: {name} at: {path}'
        """
        return 'git repository: %s at: %s' % (self._repo_name(), self.git_dir,)

    def __repr__(self):
        """Returns a unique string representation of the repository.

        Returns:
            str: The absolute path to the repository
        """
        return str(self.git_dir)

    def bus_factor(self, by='repository', ignore_globs=None, include_globs=None):
        """Calculates the "bus factor" for the repository.

        The bus factor is a measure of risk based on how concentrated the codebase knowledge is
        among contributors. It is calculated as the minimum number of contributors whose combined
        contributions account for at least 50% of the codebase's lines of code.

        Args:
            by (str, optional): How to calculate the bus factor. One of:
                - 'repository': Calculate for entire repository (default)
                - 'file': Not implemented yet
            ignore_globs (Optional[List[str]]): List of glob patterns for files to ignore
            include_globs (Optional[List[str]]): List of glob patterns for files to include

        Returns:
            pandas.DataFrame: A DataFrame with columns:
                - repository (str): Repository name
                - bus factor (int): Bus factor for the repository

        Raises:
            NotImplementedError: If by='file' is specified (not implemented yet)

        Note:
            A low bus factor (e.g. 1-2) indicates high risk as knowledge is concentrated among
            few contributors. A higher bus factor indicates knowledge is better distributed.
        """

        if by == 'file':
            raise NotImplementedError('File-wise bus factor')

        blame = self.blame(include_globs=include_globs, ignore_globs=ignore_globs, by=by)
        blame = blame.sort_values(by=['loc'], ascending=False)

        total = blame['loc'].sum()
        cumulative = 0
        tc = 0
        for idx in range(blame.shape[0]):
            cumulative += blame.iloc[idx]["loc"]
            tc += 1
            if cumulative >= total / 2:
                break

        return DataFrame([[self._repo_name(), tc]], columns=['repository', 'bus factor'])

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
            Optional[str]: Name of the primary owner, or None if file doesn't exist
                or can't be analyzed

        Note:
            This is a helper method used by file_detail() to determine file ownership.
        """
        try:
            if committer:
                cm = 'committer'
            else:
                cm = 'author'

            blame = self.repo.blame(rev, os.path.join(self.git_dir, filename))
            blame = DataFrame([[x[0].committer.name, len(x[1])] for x in blame], columns=[cm, 'loc']).groupby(cm).agg(
                {'loc': np.sum})
            if blame.shape[0] > 0:
                return blame['loc'].idxmax()
            else:
                return None
        except (GitCommandError, KeyError):
            if self.verbose:
                print('Couldn\'t Calcualte File Owner for %s' % (rev,))
            return None

    def _file_last_edit(self, filename):
        """Gets the timestamp of the last modification to a file.

        Args:
            filename (str): Path to the file relative to repository root

        Returns:
            Optional[str]: Date string from git log, or None if file doesn't exist
                or has no history

        Note:
            This is an internal helper method used by file_detail() to get file
            modification times.
        """

        tmp = self.repo.git.log('-n 1 -- %s' % (filename,)).split('\n')
        date_string = [x for x in tmp if x.startswith('Date:')]

        if len(date_string) > 0:
            return date_string[0].replace('Date:', '').strip()
        else:
            return None

    @multicache(
        key_prefix='file_detail',
        key_list=['include_globs', 'ignore_globs', 'rev', 'committer'],
        skip_if=lambda x: True if x.get('rev') is None or x.get('rev') == 'HEAD' else False
    )
    def file_detail(self, include_globs=None, ignore_globs=None, rev='HEAD', committer=True):
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
            pandas.DataFrame: A DataFrame indexed by file path with columns:
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

        # first get the blame
        blame = self.blame(
            include_globs=include_globs,
            ignore_globs=ignore_globs,
            rev=rev,
            committer=committer,
            by='file'
        )
        blame = blame.reset_index(level=-1)
        blame = blame.reset_index(level=-1)

        # reduce it to files and total LOC
        df = blame.reindex(columns=['file', 'loc'])
        df = df.groupby('file').agg({'loc': np.sum})
        df = df.reset_index(level=-1)

        # map in file owners
        df['file_owner'] = df['file'].map(lambda x: self.file_owner(rev, x, committer=committer))

        # add extension (something like the language)
        df['ext'] = df['file'].map(lambda x: x.split('.')[-1])

        # add in last edit date for the file
        df['last_edit_date'] = df['file'].map(self._file_last_edit)
        df['last_edit_date'] = to_datetime(df['last_edit_date'])

        df = df.set_index('file')
        df = self._add_labels_to_df(df)

        return df

    def punchcard(self, branch='master', limit=None, days=None, by=None, normalize=None, ignore_globs=None,
                  include_globs=None):
        """
        Returns a pandas DataFrame containing all of the data for a punchcard.

         * day_of_week
         * hour_of_day
         * author / committer
         * lines
         * insertions
         * deletions
         * net

        :param branch: the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param days: (optional, default=None) number of days to return, if limit is None
        :param by: (optional, default=None) agg by options, None for no aggregation (just a high level punchcard), or 'committer', 'author'
        :param normalize: (optional, default=None) if an integer, returns the data normalized to max value of that (for plotting)
        :param ignore_globs: (optional, default=None) a list of globs to ignore, default none excludes nothing
        :param include_globs: (optinal, default=None) a list of globs to include, default of None includes everything.
        :return: DataFrame
        """

        ch = self.commit_history(
            branch=branch,
            limit=limit,
            days=days,
            ignore_globs=ignore_globs,
            include_globs=include_globs
        )

        # add in the date fields
        ch['day_of_week'] = ch.index.map(lambda x: x.weekday())
        ch['hour_of_day'] = ch.index.map(lambda x: x.hour)

        aggs = ['hour_of_day', 'day_of_week']
        if by is not None:
            aggs.append(by)

        punch_card = ch.groupby(aggs).agg({
            'lines': np.sum,
            'insertions': np.sum,
            'deletions': np.sum,
            'net': np.sum
        })
        punch_card.reset_index(inplace=True)

        # normalize all cols
        if normalize is not None:
            for col in ['lines', 'insertions', 'deletions', 'net']:
                punch_card[col] = (punch_card[col] / punch_card[col].sum()) * normalize

        return punch_card


class GitFlowRepository(Repository):
    """
    A special case where git flow is followed, so we know something about the branching scheme
    """

    def __init__(self):
        super(GitFlowRepository, self).__init__()
