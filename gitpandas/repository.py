"""
.. module:: repository
   :platform: Unix, Windows
   :synopsis: A module for examining a single git repository

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""


import os
import sys
import datetime
import numpy as np
import logging
from git import Repo, GitCommandError
from pandas import DataFrame, to_datetime

__author__ = 'willmcginnis'


class Repository(object):
    """
    The base class for a generic git repository, from which to gather statistics.  The object encapulates a single
    gitpython Repo instance.

    :param working_dir: the directory of the git repository, meaning a .git directory is in it (default None=cwd)
    :return:
    """

    def __init__(self, working_dir=None, verbose=False):
        self.verbose = verbose
        self.log = logging.getLogger('gitpandas')

        if working_dir is not None:
            self.git_dir = working_dir
        else:
            self.git_dir = os.getcwd()

        self.repo = Repo(self.git_dir)

        if self.verbose:
            print('Repository [%s] instantiated at directory: %s' % (self._repo_name(), self.git_dir))

    def is_bare(self):
        """
        Returns a boolean for if the repo is bare or not

        :return: bool
        """

        return self.repo.bare

    def commit_history(self, branch, limit=None, extensions=None, ignore_dir=None):
        """
        Returns a pandas DataFrame containing all of the commits for a given branch. Included in that DataFrame will be
        the columns:

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
        :return: DataFrame
        """

        # setup the data-set of commits
        if limit is None:
            ds = [[
                      x.author.name,
                      x.committer.name,
                      x.committed_date,
                      x.message,
                      self.__check_extension(x.stats.files, extensions, ignore_dir)
                  ] for x in self.repo.iter_commits(branch, max_count=sys.maxsize)]
        else:
            ds = [[
                      x.author.name,
                      x.committer.name,
                      x.committed_date,
                      x.message,
                      self.__check_extension(x.stats.files, extensions, ignore_dir)
                  ] for x in self.repo.iter_commits(branch, max_count=limit)]

        # aggregate stats
        ds = [x[:-1] + [sum([x[-1][key]['lines'] for key in x[-1].keys()]),
                       sum([x[-1][key]['insertions'] for key in x[-1].keys()]),
                       sum([x[-1][key]['deletions'] for key in x[-1].keys()]),
                       sum([x[-1][key]['insertions'] for key in x[-1].keys()]) - sum([x[-1][key]['deletions'] for key in x[-1].keys()])
                       ] for x in ds if len(x[-1].keys()) > 0]

        # make it a pandas dataframe
        df = DataFrame(ds, columns=['author', 'committer', 'date', 'message', 'lines', 'insertions', 'deletions', 'net'])

        # format the date col and make it the index
        df['date'] = to_datetime(df['date'].map(lambda x: datetime.datetime.fromtimestamp(x)))
        df.set_index(keys=['date'], drop=True, inplace=True)

        return df

    def file_change_history(self, branch, limit=None, extensions=None, ignore_dir=None):
        """
        Returns a DataFrame of all file changes (via the commit history) for the specified branch.  This is similar to
        the commit history DataFrame, but is one row per file edit rather than one row per commit (which may encapsulate
        many file changes). Included in the DataFrame will be the columns:

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
        :return: DataFrame
        """

        # setup the dataset of commits
        if limit is None:
            ds = [[
                      x.author.name,
                      x.committer.name,
                      x.committed_date,
                      x.message,
                      self.__check_extension(x.stats.files, extensions, ignore_dir)
                  ] for x in self.repo.iter_commits(branch, max_count=sys.maxsize)]
        else:
            ds = [[
                      x.author.name,
                      x.committer.name,
                      x.committed_date,
                      x.message,
                      self.__check_extension(x.stats.files, extensions, ignore_dir)
                  ] for x in self.repo.iter_commits(branch, max_count=limit)]

        ds = [x[:-1] + [fn, x[-1][fn]['insertions'], x[-1][fn]['deletions']] for x in ds for fn in x[-1].keys() if len(x[-1].keys()) > 0]

        # make it a pandas dataframe
        df = DataFrame(ds, columns=['author', 'committer', 'date', 'message', 'filename', 'insertions', 'deletions'])

        # format the date col and make it the index
        df['date'] = to_datetime(df['date'].map(lambda x: datetime.datetime.fromtimestamp(x)))
        df.set_index(keys=['date'], drop=True, inplace=True)

        return df

    @staticmethod
    def __check_extension(files, extensions, ignore_dir):
        """
        Internal method to filter a list of file changes by extension and ignore_dirs.

        :param files:
        :param extensions: a list of file extensions to return commits for
        :param ignore_dir: a list of directory names to ignore
        :return: dict
        """

        if extensions is None:
            return files

        if ignore_dir is None:
            ignore_dir = []
        else:
            ignore_dir = [os.sep + str(x).replace('/', '').replace('\\', '') + os.sep for x in ignore_dir]

        out = {}
        for key in files.keys():
            if key.split('.')[-1] in extensions:
                if sum([1 if x in key else 0 for x in ignore_dir]) == 0:
                    out[key] = files[key]

        return out

    def blame(self, extensions=None, ignore_dir=None, rev='HEAD'):
        """
        Returns the blame from the current HEAD of the repository as a DataFrame.  The DataFrame is grouped by committer
        name, so it will be the sum of all contributions to the repository by each committer. As with the commit history
        method, extensions and ignore_dirs parameters can be passed to exclude certain directories, or focus on certain
        file extensions. The DataFrame will have the columns:

         * committer
         * loc

        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :return: DataFrame
        """

        if ignore_dir is None:
            ignore_dir = []

        blames = []
        for roots, dirs, files in os.walk(self.git_dir):
            if '.git' not in roots and sum([1 if x in roots else 0 for x in ignore_dir]) == 0:
                if extensions is not None:
                    filenames = [roots + os.sep + x for x in files if x.split('.')[-1] in extensions]
                else:
                    filenames = [roots + os.sep + x for x in files]

                for file in filenames:
                    try:
                        blames.append(self.repo.blame(rev, str(file).replace(self.git_dir + '/', '')))
                    except GitCommandError as err:
                        pass

        blames = [item for sublist in blames for item in sublist]
        blames = DataFrame([[x[0].committer.name, len(x[1])] for x in blames], columns=['committer', 'loc']).groupby('committer').agg({'loc': np.sum})

        return blames

    def revs(self, branch='master', limit=None, skip=None):
        """
        Returns a dataframe of all revision tags and their timestamps. It will have the columns:

         * date
         * rev

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :return: DataFrame

        """

        if limit is None:
            limit = sys.maxsize
        elif skip is not None:
            limit = limit * skip

        ds = [[x.committed_date, x.name_rev.split(' ')[0]] for x in self.repo.iter_commits(branch, max_count=limit)]
        df = DataFrame(ds, columns=['date', 'rev'])

        if skip is not None:
            if df.shape[0] >= skip:
                df = df.ix[range(0, df.shape[0], skip)]
                df.reset_index()
            else:
                df = df.ix[[0]]
                df.reset_index()

        return df

    def cumulative_blame(self, branch='master', extensions=None, ignore_dir=None, limit=None, skip=None):
        """
        Returns the blame at every revision of interest. Index is a datetime, column per committer, with number of lines
        blamed to each committer at each timestamp as data.

        :param branch: (optional, default 'master') the branch to work in
        :param limit: (optional, default None), the maximum number of revisions to return, None for no limit
        :param skip: (optional, default None), the number of revisions to skip. Ex: skip=2 returns every other revision, None for no skipping.
        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :return: DataFrame

        """

        if limit is None:
            limit = sys.maxsize

        revs = self.revs(branch=branch, limit=limit, skip=skip)

        # get the commit history to stub out committers (hacky and slow)
        committers = {x.committer.name for x in self.repo.iter_commits(branch, max_count=sys.maxsize)}
        for committer in committers:
            revs[committer] = 0

        if self.verbose:
            print('Beginning processing for cumulative blame:')

        # now populate that table with some actual values
        for idx, row in revs.iterrows():
            if self.verbose:
                print('%s. [%s] getting blame for rev: %s' % (str(idx), datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), row.rev, ))

            blame = self.blame(extensions=extensions, ignore_dir=ignore_dir, rev=row.rev)
            for committer in committers:
                try:
                    loc = blame.loc[committer, 'loc']
                    revs.set_value(idx, committer, loc)
                except KeyError:
                    pass

        del revs['rev']

        revs['date'] = to_datetime(revs['date'].map(lambda x: datetime.datetime.fromtimestamp(x)))
        revs.set_index(keys=['date'], drop=True, inplace=True)

        revs = revs.fillna(0.0)
        return revs

    def branches(self):
        """
        Returns a data frame of all branches in origin.  The DataFrame will have the columns:

         * repository
         * branch

        :returns: DataFrame
        """

        branches = self.repo.branches
        df = DataFrame(list(branches), columns=['branch'])
        df['repository'] = self._repo_name()

        return df

    def tags(self):
        """
        Returns a data frame of all tags in origin.  The DataFrame will have the columns:

         * repository
         * tag

        :returns: DataFrame
        """

        branches = self.repo.tags
        df = DataFrame(list(branches), columns=['tag'])
        df['repository'] = self._repo_name()

        return df

    def _repo_name(self):
        """
        Returns the name of the repository, using the local directory name.

        :returns: str
        """

        reponame = self.repo.git_dir.split(os.sep)[-2]
        if reponame.strip() == '':
            return 'unknown_repo'
        return reponame

    def __str__(self):
        """
        A pretty name for the repository object.

        :returns: str
        """
        return 'git repository: %s at: %s' % (self._repo_name(), self.git_dir, )

    def __repr__(self):
        """
        A unique name for the repository object.

        :returns: str
        """
        return str(self.git_dir)

    def bus_factor(self, extensions=None, ignore_dir=None):
        """
        An experimental heuristic for truck factor of a repository calculated by the current distribution of blame in
        the repository's primary branch.  The factor is the fewest number of contributors whose contributions make up at
        least 50% of the codebase's LOC
        :param branch:
        :return:
        """

        blame = self.blame(extensions=extensions, ignore_dir=ignore_dir)
        blame = blame.sort_values(by=['loc'], ascending=False)

        total = blame['loc'].sum()
        cumulative = 0
        tc = 0
        for idx in range(blame.shape[0]):
            cumulative += blame.ix[idx, 'loc']
            tc += 1
            if cumulative >= total / 2:
                break

        return tc


class GitFlowRepository(Repository):
    """
    A special case where git flow is followed, so we know something about the branching scheme
    """
    def __init__(self):
        super(Repository, self).__init__()


