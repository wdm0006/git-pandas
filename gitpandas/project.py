"""
.. module:: projectdirectory
   :platform: Unix, Windows
   :synopsis: A module for examining collections of git repositories as a whole

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

import math
import sys
import os
import numpy as np
import pandas as pd
import requests
import warnings
from git import GitCommandError
from gitpandas.repository import Repository

__author__ = 'willmcginnis'


class ProjectDirectory(object):
    """
    An object that refers to a directory full of git repositories, for bulk analysis.  It contains a collection of
    git-pandas repository objects, created by os.walk-ing a directory to file all child .git subdirectories.

    :param working_dir: (optional, default=None), the working directory to search for repositories in, None for cwd, or an explicit list of directories containing git repositories
    :param ignore: (optional, default=None), a list of directories to ignore when searching for git repos.
    :param verbose: (default=True), if True, will print out verbose logging to terminal
    :return:
    """
    def __init__(self, working_dir=None, ignore_repos=None, verbose=True):
        if working_dir is None:
            self.repo_dirs = set([x[0].split('.git')[0] for x in os.walk(os.getcwd()) if '.git' in x[0]])
        elif isinstance(working_dir, list):
            self.repo_dirs = working_dir
        else:
            self.repo_dirs = set([x[0].split('.git')[0] for x in os.walk(working_dir) if '.git' in x[0]])

        self.repos = [Repository(r, verbose=verbose) for r in self.repo_dirs]

        if ignore_repos is not None:
            self.repos = [x for x in self.repos if x.repo_name not in ignore_repos]

    def _repo_name(self):
        warnings.warn('please use repo_name() now instead of _repo_name()', DeprecationWarning)
        return self.repo_name()

    def repo_name(self):
        """
        Returns a DataFrame of the repo names present in this project directory

        :return: DataFrame

        """

        ds = [[x.repo_name] for x in self.repos]
        df = pd.DataFrame(ds, columns=['repository'])
        return df

    def is_bare(self):
        """
        Returns a dataframe of repo names and whether or not they are bare.

        :return: DataFrame
        """

        ds = [[x.repo_name, x.is_bare()] for x in self.repos]
        df = pd.DataFrame(ds, columns=['repository', 'is_bare'])
        return df

    def has_coverage(self):
        """
        Returns a DataFrame of repo names and whether or not they have a .coverage file that can be parsed

        :return: DataFrame
        """

        ds = [[x.repo_name, x.has_coverage()] for x in self.repos]
        df = pd.DataFrame(ds, columns=['repository', 'has_coverage'])
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

        df = pd.DataFrame(columns=['filename', 'lines_covered', 'total_lines', 'coverage', 'repository'])

        for repo in self.repos:
            try:
                cov = repo.coverage()
                cov['repository'] = repo.repo_name
                df = df.append(cov)
            except GitCommandError:
                print('Warning! Repo: %s seems to not have coverage' % (repo, ))

        df.reset_index()

        return df

    def file_change_rates(self, branch='master', limit=None, extensions=None, ignore_dir=None, coverage=False, days=None, ignore_globs=None):
        """
        This function will return a DataFrame containing some basic aggregations of the file change history data, and
        optionally test coverage data from a coverage_data.py .coverage file.  The aim here is to identify files in the
        project which have abnormal edit rates, or the rate of changes without growing the files size.  If a file has
        a high change rate and poor test coverage, then it is a great candidate for writing more tests.

        :param branch: (optional, default=master) the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param coverage: (optional, default=False) a bool for whether or not to attempt to join in coverage data.
        :param days: (optional, default=None) number of days to return if limit is None
        :param ignore_globs: (optional, default=None) a list of globs to ignore, replaces extensions and ignore_dir
        :return: DataFrame
        """

        columns = ['unique_committers', 'abs_rate_of_change', 'net_rate_of_change', 'net_change', 'abs_change', 'edit_rate', 'repository']
        if coverage:
            columns += ['lines_covered', 'total_lines', 'coverage']
        df = pd.DataFrame(columns=columns)

        for repo in self.repos:
            try:
                fcr = repo.file_change_rates(
                    branch=branch,
                    limit=limit,
                    extensions=extensions,
                    ignore_dir=ignore_dir,
                    coverage=coverage,
                    days=days,
                    ignore_globs=ignore_globs
                )
                fcr['repository'] = repo.repo_name
                df = df.append(fcr)
            except GitCommandError:
                print('Warning! Repo: %s seems to not have the branch: %s' % (repo, branch))

        df.reset_index()

        return df

    def hours_estimate(self, branch='master', grouping_window=0.5, single_commit_hours=0.5, limit=None, extensions=None, ignore_dir=None, days=None, committer=True, by=None, ignore_globs=None):
        """
        inspired by: https://github.com/kimmobrunfeldt/git-hours/blob/8aaeee237cb9d9028e7a2592a25ad8468b1f45e4/index.js#L114-L143

        Iterates through the commit history of repo to estimate the time commitement of each author or committer over
        the course of time indicated by limit/extensions/days/etc.

        :param branch: the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param grouping_window: (optional, default=0.5 hours) the threhold for how close two commits need to be to consider them part of one coding session
        :param single_commit_hours: (optional, default 0.5 hours) the time range to associate with one single commit
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param days: (optional, default=None) number of days to return, if limit is None
        :param committer: (optional, default=True) whether to use committer vs. author
        :param ignore_globs: (optional, default=None) a list of globs to ignore, replaces extensions and ignore_dir
        :return: DataFrame
        """

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        if committer:
            com = 'committer'
        else:
            com = 'author'

        df = pd.DataFrame(columns=[com, 'hours', 'repository'])

        for repo in self.repos:
            try:
                ch = repo.hours_estimate(
                    branch,
                    grouping_window=grouping_window,
                    single_commit_hours=single_commit_hours,
                    limit=limit,
                    extensions=extensions,
                    ignore_dir=ignore_dir,
                    days=days,
                    committer=committer,
                    ignore_globs=ignore_globs
                )
                ch['repository'] = repo.repo_name
                df = df.append(ch)
            except GitCommandError:
                print('Warning! Repo: %s seems to not have the branch: %s' % (repo, branch))

        df.reset_index()

        if by == 'committer' or by == 'author':
            df = df.groupby(com).agg({'hours': sum})
            df = df.reset_index()
        elif by == 'repository':
            df = df.groupby('repository').agg({'hours': sum})
            df = df.reset_index()

        return df

    def commit_history(self, branch, limit=None, extensions=None, ignore_dir=None, days=None, ignore_globs=None):
        """
        Returns a pandas DataFrame containing all of the commits for a given branch. The results from all repositories
        are appended to each other, resulting in one large data frame of size <limit>.  If a limit is provided, it is
        divided by the number of repositories in the project directory to find out how many commits to pull from each
        project. Future implementations will use date ordering across all projects to get the true most recent N commits
        across the project.

        Included in that DataFrame will be the columns:

         * repository
         * date (index)
         * author
         * committer
         * message
         * lines
         * insertions
         * deletions
         * net

        :param branch: the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param days: (optional, default=None) number of days to return if limit is None
        :param ignore_globs: (optional, default=None) a list of globs to ignore, replaces extensions and ignore_dir
        :return: DataFrame
        """

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        df = pd.DataFrame(columns=['author', 'committer', 'message', 'lines', 'insertions', 'deletions', 'net'])

        for repo in self.repos:
            try:
                ch = repo.commit_history(branch, limit=limit, extensions=extensions, ignore_dir=ignore_dir, days=days, ignore_globs=ignore_globs)
                ch['repository'] = repo.repo_name
                df = df.append(ch)
            except GitCommandError:
                print('Warning! Repo: %s seems to not have the branch: %s' % (repo, branch))

        df.reset_index()

        return df

    def file_change_history(self, branch='master', limit=None, extensions=None, ignore_dir=None, days=None, ignore_globs=None):
        """
        Returns a DataFrame of all file changes (via the commit history) for the specified branch.  This is similar to
        the commit history DataFrame, but is one row per file edit rather than one row per commit (which may encapsulate
        many file changes). Included in the DataFrame will be the columns:

         * repository
         * date (index)
         * author
         * committer
         * message
         * filename
         * insertions
         * deletions

        :param branch: the branch to return commits for
        :param limit: (optional, default=None) a maximum number of commits to return, None for no limit
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param days: (optional, default=None) number of days to return if limit is None
        :param ignore_globs: (optional, default=None) a list of globs to ignore, replaces extensions and ignore_dir
        :return: DataFrame
        """

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        df = pd.DataFrame(columns=['repository', 'date', 'author', 'committer', 'message', 'rev', 'filename', 'insertions', 'deletions'])

        for repo in self.repos:
            try:
                ch = repo.file_change_history(
                    branch,
                    limit=limit,
                    extensions=extensions,
                    ignore_dir=ignore_dir,
                    days=days,
                    ignore_globs=ignore_globs
                )
                ch['repository'] = repo.repo_name
                df = df.append(ch)
            except GitCommandError:
                print('Warning! Repo: %s seems to not have the branch: %s' % (repo, branch))

        df.reset_index()

        return df

    def blame(self, extensions=None, ignore_dir=None, committer=True, by='repository'):
        """
        Returns the blame from the current HEAD of the repositories as a DataFrame.  The DataFrame is grouped by committer
        name, so it will be the sum of all contributions to all repositories by each committer. As with the commit history
        method, extensions and ignore_dirs parameters can be passed to exclude certain directories, or focus on certain
        file extensions. The DataFrame will have the columns:

         * committer
         * loc

        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param committer: (optional, default=True) true if committer should be reported, false if author
        :param by: (optional, default=repository) whether to group by repository or by file
        :return: DataFrame
        """

        df = None

        for repo in self.repos:
            try:
                if df is None:
                    df = repo.blame(extensions=extensions, ignore_dir=ignore_dir, committer=committer, by=by)
                else:
                    df = df.append(repo.blame(extensions=extensions, ignore_dir=ignore_dir, committer=committer, by=by))
            except GitCommandError as err:
                print('Warning! Repo: %s couldnt be blamed' % (repo, ))
                pass

        df = df.reset_index(level=1)
        df = df.reset_index(level=1)
        if committer:
            if by == 'repository':
                df = df.groupby('committer').agg({'loc': np.sum})
            elif by == 'file':
                df = df.groupby(['committer', 'file']).agg({'loc': np.sum})
        else:
            if by == 'repository':
                df = df.groupby('author').agg({'loc': np.sum})
            elif by == 'file':
                df = df.groupby(['author', 'file']).agg({'loc': np.sum})

        df = df.sort_values(by=['loc'], ascending=False)

        return df

    def file_detail(self, extensions=None, ignore_dir=None, rev='HEAD', committer=True):
        """
        Returns a table of all current files in the repos, with some high level information about each file (total LOC,
        file owner, extension, most recent edit date, etc.).

        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param committer: (optional, default=True) true if committer should be reported, false if author
        :return:
        """

        df = None

        for repo in self.repos:
            try:
                if df is None:
                    df = repo.file_detail(extensions=extensions, ignore_dir=ignore_dir, committer=committer, rev=rev)
                    df['repository'] = repo.repo_name
                else:
                    chunk = repo.file_detail(extensions=extensions, ignore_dir=ignore_dir, committer=committer, rev=rev)
                    chunk['repository'] = repo.repo_name
                    df = df.append(chunk)
            except GitCommandError:
                print('Warning! Repo: %s couldnt be inspected' % (repo, ))

        df = df.reset_index(level=1)
        df = df.set_index(['file', 'repository'])
        return df

    def branches(self):
        """
        Returns a data frame of all branches in origin.  The DataFrame will have the columns:

         * repository
         * local
         * branch

        :returns: DataFrame
        """

        df = pd.DataFrame(columns=['repository', 'local', 'branch'])

        for repo in self.repos:
            try:
                df = df.append(repo.branches())
            except GitCommandError:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))

        df.reset_index()

        return df

    def revs(self, branch='master', limit=None, skip=None, num_datapoints=None):
        """
        Returns a dataframe of all revision tags and their timestamps for each project. It will have the columns:

         * date
         * repository
         * rev

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :param num_datapoints: (optional, default=None) if limit and skip are none, and this isn't, then num_datapoints evenly spaced revs will be used

        :return: DataFrame
        """

        if limit is not None:
            limit = math.floor(float(limit) / len(self.repos))

        if num_datapoints is not None:
            num_datapoints = math.floor(float(num_datapoints) / len(self.repos))

        df = pd.DataFrame(columns=['repository', 'rev'])

        for repo in self.repos:
            try:
                revs = repo.revs(branch=branch, limit=limit, skip=skip, num_datapoints=num_datapoints)
                revs['repository'] = repo.repo_name
                df = df.append(revs)
            except GitCommandError:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))

        df.reset_index()

        return df

    def cumulative_blame(self, branch='master', extensions=None, ignore_dir=None, by='committer', limit=None, skip=None, num_datapoints=None, committer=True):
        """
        Returns a time series of cumulative blame for a collection of projects.  The goal is to return a dataframe for a
        collection of projects with the LOC attached to an entity at each point in time. The returned dataframe can be
        returned in 3 forms (switched with the by parameter, default 'committer'):

         * committer: one column per committer
         * project: one column per project
         * raw: one column per committed per project

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param num_datapoints: (optional, default=None) if limit and skip are none, and this isn't, then num_datapoints evenly spaced revs will be used
        :param committer: (optional, default=True) true if committer should be reported, false if author
        :param by: (optional, default='committer') whether to arrange the output by committer or project
        :return: DataFrame

        """

        blames = []
        for repo in self.repos:
            try:
                blame = repo.cumulative_blame(
                    branch=branch,
                    extensions=extensions,
                    ignore_dir=ignore_dir,
                    limit=limit,
                    skip=skip,
                    num_datapoints=num_datapoints,
                    committer=committer
                )
                blames.append((repo.repo_name, blame))
            except GitCommandError:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))
                pass

        global_blame = blames[0][1]
        global_blame.columns = [x + '__' + str(blames[0][0]) for x in global_blame.columns.values]
        blames = blames[1:]
        for reponame, blame in blames:
            blame.columns = [x + '__' + reponame for x in blame.columns.values]
            global_blame = pd.merge(global_blame, blame, left_index=True, right_index=True, how='outer')

        global_blame.fillna(method='pad', inplace=True)
        global_blame.fillna(0.0, inplace=True)

        if by == 'committer':
            committers = [(str(x).split('__')[0].lower().strip(), x) for x in global_blame.columns.values]

            if sys.version_info.major == 2:
                committer_mapping = dict([(c, [x[1] for x in committers if x[0] == c]) for c in set([x[0] for x in committers])])
            else:
                committer_mapping = {c: [x[1] for x in committers if x[0] == c] for c in {x[0] for x in committers}}

            for committer in committer_mapping.keys():
                global_blame[committer] = 0
                for col in committer_mapping.get(committer, []):
                    global_blame[committer] += global_blame[col]

            global_blame = global_blame.reindex(columns=list(committer_mapping.keys()))
        elif by == 'project':
            projects = [(str(x).split('__')[1].lower().strip(), x) for x in global_blame.columns.values]

            if sys.version_info.major == 2:
                project_mapping = dict([(c, [x[1] for x in projects if x[0] == c]) for c in set([x[0] for x in projects])])
            else:
                project_mapping = {c: [x[1] for x in projects if x[0] == c] for c in {x[0] for x in projects}}

            for project in project_mapping.keys():
                global_blame[project] = 0
                for col in project_mapping.get(project, []):
                    global_blame[project] += global_blame[col]

            global_blame = global_blame.reindex(columns=list(project_mapping.keys()))

        global_blame = global_blame[~global_blame.index.duplicated()]

        return global_blame

    def tags(self):
        """
        Returns a data frame of all tags in origin.  The DataFrame will have the columns:

         * repository
         * tag

        :returns: DataFrame
        """

        df = pd.DataFrame(columns=['repository', 'tag'])

        for repo in self.repos:
            try:
                df = df.append(repo.tags())
            except GitCommandError:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))

        df.reset_index()

        return df

    def repo_information(self):
        """
        Returns a DataFrame with the properties of all repositories in the project directory. The returned DataFrame
        will have the columns:

         * local_directory
         * branches
         * bare
         * remotes
         * description
         * references
         * heads
         * submodules
         * tags
         * active_branch

        :return: DataFrame
        """

        data = [[repo.git_dir,
                 repo.repo.branches,
                 repo.repo.bare,
                 repo.repo.remotes,
                 repo.repo.description,
                 repo.repo.references,
                 repo.repo.heads,
                 repo.repo.submodules,
                 repo.repo.tags,
                 repo.repo.active_branch] for repo in self.repos]

        df = pd.DataFrame(data, columns=[
            'local_directory',
            'branches',
            'bare',
            'remotes',
            'description',
            'references',
            'heads',
            'submodules',
            'tags',
            'active_branch'
        ])

        return df

    def bus_factor(self, extensions=None, ignore_dir=None, by='projectd'):
        """
        An experimental heuristic for truck factor of a repository calculated by the current distribution of blame in
        the repository's primary branch.  The factor is the fewest number of contributors whose contributions make up at
        least 50% of the codebase's LOC

        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore

        :return:
        """

        if by == 'file':
            raise NotImplementedError('File-wise bus factor')
        elif by == 'projectd':
            blame = self.blame(extensions=extensions, ignore_dir=ignore_dir, by='repository')
            blame = blame.sort_values(by=['loc'], ascending=False)

            total = blame['loc'].sum()
            cumulative = 0
            tc = 0
            for idx in range(blame.shape[0]):
                cumulative += blame.ix[idx, 'loc']
                tc += 1
                if cumulative >= total / 2:
                    break

            return pd.DataFrame([['projectd', tc]], columns=['projectd', 'bus factor'])
        elif by == 'repository':
            df = pd.DataFrame(columns=['repository', 'bus factor'])
            for repo in self.repos:
                try:
                    df = df.append(repo.bus_factor(extensions=extensions, ignore_dir=ignore_dir, by=by))
                except GitCommandError:
                    print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))

            df.reset_index()
            return df

    def punchcard(self, branch='master', limit=None, extensions=None, ignore_dir=None, days=None, by=None, normalize=None, ignore_globs=None):
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
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :param days: (optional, default=None) number of days to return, if limit is None
        :param by: (optional, default=None) agg by options, None for no aggregation (just a high level punchcard), or 'committer', 'author', 'repository'
        :param normalize: (optional, default=None) if an integer, returns the data normalized to max value of that (for plotting)
        :param ignore_globs: (optional, default=None) a list of globs to ignore, replaces extensions and ignore_dir
        :return: DataFrame
        """

        df = pd.DataFrame()

        if by == 'repository':
            repo_by = None
        else:
            repo_by = by

        for repo in self.repos:
            try:
                chunk = repo.punchcard(
                    branch=branch,
                    limit=limit,
                    extensions=extensions,
                    ignore_dir=ignore_dir,
                    days=days,
                    by=repo_by,
                    normalize=None,
                    ignore_globs=ignore_globs
                )
                chunk['repository'] = repo.repo_name
                df = df.append(chunk)
            except GitCommandError:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))

        df.reset_index()

        aggs = ['hour_of_day', 'day_of_week']
        if by is not None:
            aggs.append(by)

        punch_card = df.groupby(aggs).agg({
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

    def __del__(self):
        """

        :return:
        """

        for repo in self.repos:
            repo.__del__()


class GitHubProfile(ProjectDirectory):
    """
    An extension of the ProjectDirectory object that is based off of a single github.com user's public profile.
    """
    def __init__(self, username, ignore_forks=False, ignore_repos=None, verbose=False):
        """

        :param username:
        :return:
        """

        # pull the git urls from github's api
        uri = 'https://api.github.com/users/%s/repos' % username
        data = requests.get(uri)
        repos = []
        for chunk in data.json():
            # if we are skipping forks
            if ignore_forks:
                if not chunk['fork']:
                    repos.append(chunk['git_url'])
            else:
                repos.append(chunk['git_url'])

        ProjectDirectory.__init__(self, working_dir=repos, ignore_repos=ignore_repos, verbose=verbose)
