import os
import unittest
from gitpandas import ProjectDirectory

__author__ = 'willmcginnis'


class TestProperties(unittest.TestCase):
    """
    For now this is using the git-python repo for tests. This probably isn't a great idea, we should really
    be either mocking the git portion, or have a known static repo in this directory to work with.

    """

    def test_repo_name(self):
        projectd = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git'], verbose=True)
        self.assertIn('git-pandas', list(projectd._repo_name()['repository'].values))

    def test_branches(self):
        projectd = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git'], verbose=True)
        branches = list(projectd.branches()['branch'].values)
        self.assertIn('master', branches)
        self.assertIn('gh-pages', branches)

    def test_tags(self):
        projectd = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git'], verbose=True)
        tags = list(projectd.tags()['tag'].values)
        self.assertIn('0.0.1', tags)
        self.assertIn('0.0.2', tags)

    def test_is_bare(self):
        projectd = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git'], verbose=True)
        for x in projectd.is_bare()['is_bare'].values:
            self.assertFalse(x)

