from git import Repo
import os
from pandas import DataFrame, to_datetime, set_option
import datetime
import sys
import numpy as np

__author__ = 'willmcginnis'


class Repository(object):
    """
    The base class for a generic git repository, from which to gather statistics.

    """

    branches = []

    def __init__(self, working_dir=None):
        """
        Constructor

        :param working_dir: the directory of the git repository (default None=cwd)
        :return:
        """

        if working_dir is not None:
            self.git_dir = working_dir
        else:
            self.git_dir = os.getcwd()

        self.repo = Repo(self.git_dir)

    def is_bare(self):
        """
        Returns a boolean for if the repo is bare or not
        :return:
        """
        return self.repo.bare

    def commit_history(self, branch, limit=None, extensions=None, ignore_dir=None):
        """
        Returns a df of commits for a given branch

        :param branch:
        :return:
        """

        # setup the dataset of commits
        if limit is None:
            ds = [[x.author.name, x.committer.name, x.committed_date, x.message, self.check_extension(x.stats.files, extensions, ignore_dir)] for x in self.repo.iter_commits(branch, max_count=sys.maxsize)]
        else:
            ds = [[x.author.name, x.committer.name, x.committed_date, x.message, self.check_extension(x.stats.files, extensions, ignore_dir)] for x in self.repo.iter_commits(branch, max_count=limit)]

        # aggregate stats
        ds = [x[:-1] + [sum([x[-1][key]['lines'] for key in x[-1].keys()]),
                       sum([x[-1][key]['insertions'] for key in x[-1].keys()]),
                       sum([x[-1][key]['deletions'] for key in x[-1].keys()]),
                       ] for x in ds if len(x[-1].keys()) > 0]

        # make it a pandas dataframe
        df = DataFrame(ds, columns=['author', 'committer', 'date', 'message', 'lines', 'insertions', 'deletions'])

        # format the date col and make it the index
        df['date'] = to_datetime(df['date'].map(lambda x: datetime.datetime.fromtimestamp(x)))
        df.set_index(keys=['date'], drop=True, inplace=True)

        return df

    def file_change_history(self, branch, limit=None, extensions=None, ignore_dir=None):
        """
        Returns a df of commits for a given branch

        :param branch:
        :return:
        """

        # setup the dataset of commits
        if limit is None:
            ds = [[x.author.name, x.committer.name, x.committed_date, x.message, self.check_extension(x.stats.files, extensions, ignore_dir)] for x in self.repo.iter_commits(branch, max_count=sys.maxsize)]
        else:
            ds = [[x.author.name, x.committer.name, x.committed_date, x.message, self.check_extension(x.stats.files, extensions, ignore_dir)] for x in self.repo.iter_commits(branch, max_count=limit)]

        ds = [x[:-1] + [fn, x[-1][fn]['insertions'], x[-1][fn]['deletions']] for x in ds for fn in x[-1].keys() if len(x[-1].keys()) > 0]

        # make it a pandas dataframe
        df = DataFrame(ds, columns=['author', 'committer', 'date', 'message', 'filename', 'insertions', 'deletions'])

        # format the date col and make it the index
        df['date'] = to_datetime(df['date'].map(lambda x: datetime.datetime.fromtimestamp(x)))
        df.set_index(keys=['date'], drop=True, inplace=True)

        return df

    @staticmethod
    def check_extension(files, extensions, ignore_dir):
        if extensions is None:
            return files

        if ignore_dir is None:
            ignore_dir = []

        out = {}
        for key in files.keys():
            if key.split('.')[-1] in extensions:
                if sum([1 if x in key else 0 for x in ignore_dir]) == 0:
                    out[key] = files[key]

        return out

    def __str__(self):
        return '%s' % (self.git_dir, )

    def __repr__(self):
        return '%s' % (self.git_dir, )


class GitFlowRepository(Repository):
    """
    A special case where git flow is followed, so we know something about the branching scheme
    """
    def __init__(self):
        super(Repository, self).__init__()


if __name__ == '__main__':
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    # build an example repository object and try some things out
    dir = ''
    ignore_dirs = [
        'docs',
        'tests',
        'Data'
    ]
    r = Repository(dir)

    # is it bare?
    print('\nRepo bare?')
    print(r.is_bare())
    print('\n')

    # get the commit history
    ch = r.commit_history('develop', limit=None, extensions=['py'], ignore_dir=ignore_dirs)
    print(ch.head(5))

    # get the list of committers
    print('\nCommiters:')
    print(''.join([str(x) + '\n' for x in set(ch['committer'].values)]))
    print('\n')

    # print out everyone's contributions
    attr = ch.reindex(columns=['committer', 'lines', 'insertions', 'deletions']).groupby(['committer'])
    attr = attr.agg({
        'lines': np.sum,
        'insertions': np.sum,
        'deletions': np.sum
    })
    print(attr)

    # get the file change history
    fh = r.file_change_history('develop', limit=None, ignore_dir=ignore_dirs)
    fh['ext'] = fh['filename'].map(lambda x: x.split('.')[-1])
    print(fh.head(50))

    # print out unique extensions
    print('\nExtensions Found:')
    print(''.join([str(x) + '\n' for x in set(fh['ext'].values)]))
    print('\n')

    # agg by extension
    etns = fh.reindex(columns=['ext', 'insertions', 'deletions']).groupby(['ext'])
    etns = etns.agg({
        'insertions': np.sum,
        'deletions': np.sum
    })
    print(etns)
