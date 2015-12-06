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
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        projectd = ProjectDirectory(working_dir=[repo_path], verbose=True)
        self.assertIn('git-pandas', list(projectd._repo_name()['repository'].values))

    def test_branches(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        projectd = ProjectDirectory(working_dir=[repo_path], verbose=True)
        branches = list(projectd.branches()['branch'].values)
        self.assertIn('master', branches)
        self.assertIn('gh-pages', branches)

    def test_tags(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        projectd = ProjectDirectory(working_dir=[repo_path], verbose=True)
        tags = list(projectd.tags()['tag'].values)
        self.assertIn('0.0.1', tags)
        self.assertIn('0.0.2', tags)

    def test_is_bare(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        projectd = ProjectDirectory(working_dir=[repo_path], verbose=True)
        for x in projectd.is_bare()['is_bare'].values:
            self.assertFalse(x)

