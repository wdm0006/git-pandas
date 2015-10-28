import os
import sys
from git import Repo, GitCommandError
import numpy as np
from pandas import DataFrame, set_option
from gitpandas.repository import Repository

__author__ = 'willmcginnis'


class ProjectDirectory(object):
    """
    An object that refers to a directory full of git repos, for bulk analysis

    """
    def __init__(self, working_dir=None):
        """

        :param dir:
        :return:
        """
        if working_dir is not None:
            self.project_dir = working_dir
        else:
            self.project_dir = os.getcwd()

        self.repo_dirs = set([x[0].split('.git')[0] for x in os.walk(self.project_dir) if '.git' in x[0]])
        self.repos = [Repository(r) for r in self.repo_dirs]

    def commit_history(self, branch, limit=None, extensions=None, ignore_dir=None):
        """

        :param branch:
        :param limit:
        :return:
        """

        if limit is not None:
            limit = int(limit / len(self.repo_dirs))

        df = DataFrame(columns=['author', 'committer', 'date', 'message', 'lines', 'insertions', 'deletions', 'net'])

        for repo in self.repos:
            try:
                df = df.append(repo.commit_history(branch, limit=limit, extensions=extensions, ignore_dir=ignore_dir))
            except GitCommandError as err:
                print('Warning! Repo: %s seems to not have the branch: %s' % (repo, branch))
                pass

        return df

if __name__ == '__main__':
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    p = ProjectDirectory(working_dir='/Users/willmcginnis/Documents/Predikto/')

    # get the commit history
    ch = p.commit_history('develop', limit=None, extensions=['py'], ignore_dir=['lib', 'docs', 'test', 'tests', 'tests_t'])
    print(ch.head(5))

    # get the list of committers
    print('\nCommiters:')
    print(''.join([str(x) + '\n' for x in set(ch['committer'].values)]))
    print('\n')

    # print out everyone's contributions
    attr = ch.reindex(columns=['committer', 'lines', 'insertions', 'deletions', 'net']).groupby(['committer'])
    attr = attr.agg({
        'lines': np.sum,
        'insertions': np.sum,
        'deletions': np.sum,
        'net': np.sum
    })
    print(attr)