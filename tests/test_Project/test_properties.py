import os
import unittest
import shutil
import time
import git
from gitpandas import ProjectDirectory

__author__ = 'willmcginnis'


class TestProperties(unittest.TestCase):
    """
    For now this is using the git-python repo for tests. This probably isn't a great idea, we should really
    be either mocking the git portion, or have a known static repo in this directory to work with.

    """

    def setUp(self):
        self.projectd = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git'], verbose=True)

    def tearDown(self):
        self.projectd.__del__()

    def test_repo_name(self):
        self.assertIn('git-pandas', list(self.projectd._repo_name()['repository'].values))

    def test_branches(self):
        branches = list(self.projectd.branches()['branch'].values)
        self.assertIn('master', branches)
        self.assertIn('gh-pages', branches)

    def test_tags(self):
        tags = list(self.projectd.tags()['tag'].values)
        self.assertIn('0.0.1', tags)
        self.assertIn('0.0.2', tags)

    def test_is_bare(self):
        for x in self.projectd.is_bare()['is_bare'].values:
            self.assertFalse(x)


class TestLocalProperties(unittest.TestCase):
    """

    """

    def setUp(self):
        """

        :return:
        """
        project_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos'
        repo1_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos' + os.sep + 'repository1'
        repo2_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos' + os.sep + 'repository2'

        if os.path.exists(project_dir):
            shutil.rmtree(project_dir)

        os.makedirs(project_dir)

        if not os.path.exists(repo1_dir):
            os.makedirs(repo1_dir)

        if not os.path.exists(repo2_dir):
            os.makedirs(repo2_dir)

        # create an empty repo (but not bare)
        grepo1 = git.Repo.init(repo1_dir)
        grepo2 = git.Repo.init(repo2_dir)

        # add a file
        with open(repo1_dir + os.sep + 'README.md', 'w') as f:
            f.write('Sample README for a sample python project\n')

        # add a file
        with open(repo2_dir + os.sep + 'README.md', 'w') as f:
            f.write('Sample README for a sample js project\n')

        # commit them
        grepo1.git.add('README.md')
        grepo1.git.commit(m='first commit')

        grepo2.git.add('README.md')
        grepo2.git.commit(m='first commit')

        # now add some other files:
        for idx in range(5):
            with open(repo1_dir + os.sep + 'file_%d.py' % (idx, ), 'w') as f:
                f.write('import sys\nimport os\n')

            time.sleep(2.0)
            grepo1.git.add(all=True)
            grepo1.git.commit(m=' "adding file_%d.py"' % (idx, ))

        # now add some other files:
        for idx in range(5):
            with open(repo2_dir + os.sep + 'file_%d.js' % (idx, ), 'w') as f:
                f.write('document.write("hello world!");\n')

            time.sleep(2.0)
            grepo2.git.add(all=True)
            grepo2.git.commit(m=' "adding file_%d.js"' % (idx, ))

        self.projectd_1 = ProjectDirectory(working_dir=[repo1_dir, repo2_dir], verbose=True)
        self.projectd_2 = ProjectDirectory(working_dir=project_dir, verbose=True)

    def tearDown(self):
        self.projectd_1.__del__()
        self.projectd_2.__del__()
        project_dir = str(os.path.dirname(os.path.abspath(__file__))) + os.sep + 'repos'
        shutil.rmtree(project_dir)

    def test_repo_name(self):
        self.assertIn('repository1', list(self.projectd_1._repo_name()['repository'].values))
        self.assertIn('repository2', list(self.projectd_1._repo_name()['repository'].values))
        self.assertIn('repository1', list(self.projectd_2._repo_name()['repository'].values))
        self.assertIn('repository2', list(self.projectd_2._repo_name()['repository'].values))

    def test_branches(self):
        branches = list(self.projectd_1.branches()['branch'].values)
        self.assertIn('master', branches)

        branches = list(self.projectd_2.branches()['branch'].values)
        self.assertIn('master', branches)

    def test_tags(self):
        tags = list(self.projectd_1.tags()['tag'].values)
        self.assertEqual(len(tags), 0)

        tags = list(self.projectd_2.tags()['tag'].values)
        self.assertEqual(len(tags), 0)

    def test_is_bare(self):
        bares = self.projectd_1.is_bare()['is_bare'].values
        for bare in bares:
            self.assertFalse(bare)

        bares = self.projectd_2.is_bare()['is_bare'].values
        for bare in bares:
            self.assertFalse(bare)

    def test_commit_history(self):
        ch = self.projectd_1.commit_history(branch='master')
        self.assertEqual(ch.shape[0], 12)

        ch2 = self.projectd_1.commit_history(branch='master', extensions=['py'])
        self.assertEqual(ch2.shape[0], 5)

        ch3 = self.projectd_1.commit_history(branch='master', limit=4)
        self.assertEqual(ch3.shape[0], 4)

        ch4 = self.projectd_1.commit_history(branch='master', days=5)
        self.assertEqual(ch4.shape[0], 12)

        fch = self.projectd_1.file_change_history(branch='master')
        self.assertEqual(fch.shape[0], 12)

        fch2 = self.projectd_1.file_change_history(branch='master', extensions=['py'])
        self.assertEqual(fch2.shape[0], 5)

        fch4 = self.projectd_1.file_change_history(branch='master', extensions=['js'])
        self.assertEqual(fch4.shape[0], 5)

        fch3 = self.projectd_1.file_change_history(branch='master', limit=4)
        self.assertEqual(fch3.shape[0], 4)

        fcr = self.projectd_1.file_change_rates(branch='master')
        self.assertEqual(fcr.shape[0], 12)
        self.assertEqual(fcr['unique_committers'].sum(), 12)
        self.assertEqual(fcr['net_change'].sum(), 17)

        # we know this repo doesnt have coverage
        coverages = self.projectd_1.has_coverage()['has_coverage'].values
        for coverage in coverages:
            self.assertFalse(coverage)

        # we know this repo only has one committer
        bf = self.projectd_1.bus_factor(by='projectd')
        self.assertEqual(bf['bus factor'].values[0], 1)

        # lets do some blaming
        blame = self.projectd_1.blame(extensions=['py'])
        self.assertEqual(blame['loc'].sum(), 10)
        self.assertEqual(blame.shape[0], 1)

        cblame = self.projectd_1.cumulative_blame(by='committer')
        self.assertEqual(cblame.shape[0], 11)
        self.assertEqual(cblame[cblame.columns.values[0]].sum(), 117)

        revs = self.projectd_1.revs(num_datapoints=2)
        self.assertEqual(revs.shape[0], 2)
        revs = self.projectd_1.revs(limit=2)
        self.assertEqual(revs.shape[0], 2)
        revs = self.projectd_1.revs()
        self.assertEqual(revs.shape[0], 12)
