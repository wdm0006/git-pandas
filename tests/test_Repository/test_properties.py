import os
import unittest
from gitpandas import Repository

__author__ = 'willmcginnis'


class TestProperties(unittest.TestCase):
    """
    For now this is using the git-python repo for tests. This probably isn't a great idea, we should really
    be either mocking the git portion, or have a known static repo in this directory to work with.

    """

    def test_repo_name(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        repo = Repository(working_dir=repo_path, verbose=True)
        self.assertEqual(repo._repo_name(), 'git-pandas')

    def test_branches(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        repo = Repository(working_dir=repo_path, verbose=True)
        branches = list(repo.branches()['branch'].values)
        self.assertIn('master', branches)
        self.assertIn('gh-pages', branches)

    def test_tags(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        repo = Repository(working_dir=repo_path, verbose=True)
        tags = list(repo.tags()['tag'].values)
        self.assertIn('0.0.1', tags)
        self.assertIn('0.0.2', tags)

    def test_is_bare(self):
        repo_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        repo = Repository(working_dir=repo_path, verbose=True)
        self.assertFalse(repo.is_bare())

