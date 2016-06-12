import os
import time
import shutil
import unittest
from gitpandas import Repository
import git

__author__ = 'willmcginnis'


class TestRemoteProperties(unittest.TestCase):
    """
    For now this is using the git-python repo for tests. This probably isn't a great idea, we should really
    be either mocking the git portion, or have a known static repo in this directory to work with.

    """

    def setUp(self):
        self.repo = Repository(working_dir='git://github.com/wdm0006/git-pandas.git', verbose=True)

    def tearDown(self):
        self.repo.__del__()

    def test_repo_name(self):
        self.assertEqual(self.repo.repo_name, 'git-pandas')

    def test_branches(self):
        branches = list(self.repo.branches()['branch'].values)
        self.assertIn('master', branches)
        self.assertIn('gh-pages', branches)

    def test_tags(self):
        tags = list(self.repo.tags()['tag'].values)
        self.assertIn('0.0.1', tags)
        self.assertIn('0.0.2', tags)

    def test_is_bare(self):
        self.assertFalse(self.repo.is_bare())


class TestLocalProperties(unittest.TestCase):
    """

    """

    def setUp(self):
        """

        :return:
        """
        project_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos'
        repo_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos' + os.sep + 'repository1'

        if os.path.exists(project_dir):
            shutil.rmtree(project_dir)

        os.makedirs(project_dir)

        if not os.path.exists(repo_dir):
            os.makedirs(repo_dir)

        # create an empty repo (but not bare)
        grepo = git.Repo.init(repo_dir)

        # add a file
        with open(repo_dir + os.sep + 'README.md', 'w') as f:
            f.write('Sample README for a sample project\n')

        # commit it
        grepo.git.add('README.md')
        grepo.git.commit(m='first commit')

        # now add some other files:
        for idx in range(5):
            with open(repo_dir + os.sep + 'file_%d.py' % (idx, ), 'w') as f:
                f.write('import sys\nimport os\n')

            time.sleep(2.0)
            grepo.git.add(all=True)
            grepo.git.commit(m='adding file_%d.py' % (idx, ))

        self.repo = Repository(working_dir=repo_dir, verbose=True)

    def tearDown(self):
        self.repo.__del__()
        project_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos'
        shutil.rmtree(project_dir)

    def test_repo_name(self):
        self.assertEqual(self.repo.repo_name, 'repository1')

    def test_branches(self):
        branches = list(self.repo.branches()['branch'].values)
        self.assertIn('master', branches)

    def test_tags(self):
        tags = list(self.repo.tags()['tag'].values)
        self.assertEqual(len(tags), 0)

    def test_is_bare(self):
        self.assertFalse(self.repo.is_bare())

    def test_commit_history(self):
        ch = self.repo.commit_history(branch='master')
        self.assertEqual(ch.shape[0], 6)

        # Will be deprecated in v2.0.0
        ch2 = self.repo.commit_history(branch='master', extensions=['py'])
        self.assertEqual(ch2.shape[0], 5)

        ch2 = self.repo.commit_history(branch='master', ignore_globs=['*.[!p][!y]'])
        self.assertEqual(ch2.shape[0], 5)

        ch3 = self.repo.commit_history(branch='master', limit=3)
        self.assertEqual(ch3.shape[0], 3)

        ch4 = self.repo.commit_history(branch='master', days=5)
        self.assertEqual(ch4.shape[0], 6)

        fch = self.repo.file_change_history(branch='master')
        self.assertEqual(fch.shape[0], 6)

        # Will be deprecated in v2.0.0
        fch2 = self.repo.file_change_history(branch='master', extensions=['py'])
        self.assertEqual(fch2.shape[0], 5)

        fch2 = self.repo.file_change_history(branch='master', ignore_globs=['*.[!p][!y]'])
        self.assertEqual(fch2.shape[0], 5)

        fch3 = self.repo.file_change_history(branch='master', limit=3)
        self.assertEqual(fch3.shape[0], 3)

        fcr = self.repo.file_change_rates(branch='master')
        self.assertEqual(fcr.shape[0], 6)
        self.assertEqual(fcr['unique_committers'].sum(), 6)
        self.assertEqual(fcr['net_change'].sum(), 11)

        # we know this repo doesnt have coverage
        self.assertFalse(self.repo.has_coverage())

        # we know this repo only has one committer
        self.assertEqual(self.repo.bus_factor(by='repository')['bus factor'].values[0], 1)

        # lets do some blaming
        blame = self.repo.blame(extensions=['py'])
        self.assertEqual(blame['loc'].sum(), 10)
        self.assertEqual(blame.shape[0], 1)

        blame = self.repo.blame(ignore_globs=['*.[!p][!y]'])
        self.assertEqual(blame['loc'].sum(), 10)
        self.assertEqual(blame.shape[0], 1)

        cblame = self.repo.cumulative_blame()
        self.assertEqual(cblame.shape[0], 6)
        self.assertEqual(cblame[cblame.columns.values[0]].sum(), 36)

        revs = self.repo.revs(num_datapoints=2)
        self.assertEqual(revs.shape[0], 2)
        revs = self.repo.revs(limit=2)
        self.assertEqual(revs.shape[0], 2)
        revs = self.repo.revs()
        self.assertEqual(revs.shape[0], 6)

