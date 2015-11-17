"""
.. module:: project
   :platform: Unix, Windows
   :synopsis: A module for examining collections of git repositories as a whole

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

import os
import numpy as np
from pandas import DataFrame
from git import GitCommandError
from gitpandas.repository import Repository

__author__ = 'willmcginnis'


class ProjectDirectory(object):
    """
    An object that refers to a directory full of git repositories, for bulk analysis.  It contains a collection of
    git-pandas repository objects, created by os.walk-ing a directory to file all child .git subdirectories.

    :param working_dir: (optional, default=None), the working directory to search for repositories in, None for cwd
    :return:
    """
    def __init__(self, working_dir=None):
        if working_dir is not None:
            self.project_dir = working_dir
        else:
            self.project_dir = os.getcwd()

        self.repo_dirs = set([x[0].split('.git')[0] for x in os.walk(self.project_dir) if '.git' in x[0]])
        self.repos = [Repository(r) for r in self.repo_dirs]

    def commit_history(self, branch, limit=None, extensions=None, ignore_dir=None):
        """
        Returns a pandas DataFrame containing all of the commits for a given branch. The results from all repositories
        are appended to each other, resulting in one large data frame of size <limit>.  If a limit is provided, it is
        divided by the number of repositories in the project directory to find out how many commits to pull from each
        project. Future implementations will use date ordering across all projects to get the true most recent N commits
        across the project.

        Included in that DataFrame will be the columns:

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
        Returns the blame from the current HEAD of the repositories as a DataFrame.  The DataFrame is grouped by committer
        name, so it will be the sum of all contributions to all repositories by each committer. As with the commit history
        method, extensions and ignore_dirs parameters can be passed to exclude certain directories, or focus on certain
        file extensions. The DataFrame will have the columns:

         * committer
         * loc

        :param extensions: (optional, default=None) a list of file extensions to return commits for
        :param ignore_dir: (optional, default=None) a list of directory names to ignore
        :return: DataFrame
        """

        df = DataFrame(columns=['loc'])

        for repo in self.repos:
            try:
                df = df.append(repo.blame(extensions=extensions, ignore_dir=ignore_dir))
            except GitCommandError as err:
                print('Warning! Repo: %s couldnt be blamed' % (repo, ))
                pass

        df = df.groupby(df.index).agg({'loc': np.sum})
        df = df.sort_values(by=['loc'], ascending=False)

        return df

    def branches(self):
        """
        Returns a data frame of all branches in origin.  The DataFrame will have the columns:

         * repository
         * branch

        :returns: DataFrame
        """

        df = DataFrame(columns=['repository', 'branch'])

        for repo in self.repos:
            try:
                df = df.append(repo.branches())
            except GitCommandError as err:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))
                pass

        return df

    def tags(self):
        """
        Returns a data frame of all tags in origin.  The DataFrame will have the columns:

         * repository
         * tag

        :returns: DataFrame
        """

        df = DataFrame(columns=['repository', 'tag'])

        for repo in self.repos:
            try:
                df = df.append(repo.tags())
            except GitCommandError as err:
                print('Warning! Repo: %s couldn\'t be inspected' % (repo, ))
                pass

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

        df = DataFrame(data, columns=[
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

    def bus_factor(self, extensions=None, ignore_dir=None):
        """
        An experimental heuristic for truck factor of a repository calculated by the current distribution of blame in
        the repository's primary branch.  The factor is the fewest number of contributors whose contributions make up at
        least 50% of the codebase's LOC
        :param branch:
        :return:
        """

        blame = self.blame(extensions=extensions, ignore_dir=ignore_dir)

        total = blame['loc'].sum()
        cumulative = 0
        tc = 0
        for idx in range(blame.shape[0]):
            cumulative += blame.ix[idx, 'loc']
            tc += 1
            if cumulative >= total / 2:
                break

        return tc