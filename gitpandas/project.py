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

    def blame(self, extensions=None, ignore_dir=None):
        """

        :param extensions:
        :param ignore_dir:
        :return:
        """

        df = DataFrame(columns=['loc'])

        for repo in self.repos:
            try:
                df = df.append(repo.blame(extensions=extensions, ignore_dir=ignore_dir))
            except GitCommandError as err:
                print('Warning! Repo: %s couldnt be blamed' % (repo, ))
                pass

        df = df.groupby(df.index).agg({'loc': np.sum})
        df = df.sort(columns=['loc'], ascending=False)

        return df

if __name__ == '__main__':
    g = ProjectDirectory(working_dir='')
    b = g.blame(extensions=['py'])
    print(b)