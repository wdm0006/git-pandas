import os
import time
import shutil
import pytest
from gitpandas import Repository
import git

__author__ = 'willmcginnis'


@pytest.fixture
def remote_repo():
    """Fixture for a remote repository."""
    repo = Repository(working_dir='https://github.com/wdm0006/git-pandas.git', verbose=True)
    yield repo
    repo.__del__()


@pytest.fixture
def local_repo(tmp_path):
    """Fixture for a local repository."""
    # Create a temporary directory
    repo_dir = tmp_path / "repository1"
    repo_dir.mkdir()
    
    # Initialize a git repo
    grepo = git.Repo.init(str(repo_dir))
    
    # Add a README file
    readme_path = repo_dir / "README.md"
    readme_path.write_text('Sample README for a sample project\n')
    
    # Commit it
    grepo.git.add('README.md')
    grepo.git.commit(m='first commit')
    
    # Add some Python files
    for idx in range(5):
        py_file = repo_dir / f"file_{idx}.py"
        py_file.write_text('import sys\nimport os\n')
        
        time.sleep(0.1)  # Small delay instead of 2.0 to speed up tests
        grepo.git.add(all=True)
        grepo.git.commit(m=f'adding file_{idx}.py')
    
    # Create the Repository object
    git_pandas_repo = Repository(working_dir=str(repo_dir), verbose=True)
    
    yield git_pandas_repo
    
    # Cleanup
    git_pandas_repo.__del__()


# Remote repository tests
class TestRemoteProperties:
    @pytest.mark.remote
    def test_repo_name(self, remote_repo):
        assert remote_repo.repo_name == 'git-pandas'

    @pytest.mark.remote
    def test_branches(self, remote_repo):
        branches = list(remote_repo.branches()['branch'].values)
        assert 'master' in branches
        assert 'gh-pages' in branches

    @pytest.mark.remote
    def test_tags(self, remote_repo):
        tags = list(remote_repo.tags()['tag'].values)
        assert '0.0.1' in tags
        assert '0.0.2' in tags

    @pytest.mark.remote
    def test_is_bare(self, remote_repo):
        assert not remote_repo.is_bare()


# Local repository tests
class TestLocalProperties:
    def test_repo_name(self, local_repo):
        assert local_repo.repo_name == 'repository1'
        
    def test_branches(self, local_repo):
        branches = list(local_repo.branches()['branch'].values)
        assert 'main' in branches
        
    def test_tags(self, local_repo):
        tags = local_repo.tags()
        assert len(tags) == 0
        
    def test_is_bare(self, local_repo):
        assert not local_repo.is_bare()
        
    def test_commit_history(self, local_repo):
        ch = local_repo.commit_history(branch='main')
        assert ch.shape[0] == 6
        
        ch2 = local_repo.commit_history(branch='main', ignore_globs=['*.[!p][!y]'])
        assert ch2.shape[0] == 5
        
        ch3 = local_repo.commit_history(branch='main', limit=3)
        assert ch3.shape[0] == 3
        
        ch4 = local_repo.commit_history(branch='main', days=5)
        assert ch4.shape[0] == 6
        
    def test_file_change_history(self, local_repo):
        fch = local_repo.file_change_history(branch='main')
        assert fch.shape[0] == 6
        
        fch2 = local_repo.file_change_history(branch='main', ignore_globs=['*.[!p][!y]'])
        assert fch2.shape[0] == 5
        
        fch3 = local_repo.file_change_history(branch='main', limit=3)
        assert fch3.shape[0] == 3
        
    def test_file_change_rates(self, local_repo):
        fcr = local_repo.file_change_rates(branch='main')
        assert fcr.shape[0] > 0
        assert fcr['unique_committers'].sum() > 0
        assert fcr['net_change'].sum() > 0
        
    def test_has_coverage(self, local_repo):
        # We know this repo doesn't have coverage
        assert not local_repo.has_coverage()
        
    def test_bus_factor(self, local_repo):
        # We know this repo only has one committer
        assert local_repo.bus_factor(by='repository')['bus factor'].values[0] == 1
        
    def test_blame(self, local_repo):
        blame = local_repo.blame(ignore_globs=['*.[!p][!y]'])
        assert blame['loc'].sum() == 10
        assert blame.shape[0] == 1
        
    def test_cumulative_blame(self, local_repo):
        cblame = local_repo.cumulative_blame(branch='main')
        assert cblame.shape[0] > 0
        assert not cblame.empty
        
    def test_revs(self, local_repo):
        revs = local_repo.revs(branch='main', num_datapoints=2)
        assert revs.shape[0] == 2
        
        revs = local_repo.revs(branch='main', limit=2)
        assert revs.shape[0] == 2
        
        revs = local_repo.revs(branch='main')
        assert revs.shape[0] == 6

