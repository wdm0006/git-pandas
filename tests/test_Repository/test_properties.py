import git
import pytest
from pandas import DataFrame

from gitpandas import Repository

__author__ = "willmcginnis"


@pytest.fixture
def remote_repo():
    """Fixture for a remote repository."""
    repo = Repository(working_dir="https://github.com/wdm0006/git-pandas.git", verbose=True)
    yield repo
    repo.__del__()


@pytest.fixture
def local_repo(tmp_path):
    """Create a local git repository for testing."""
    repo_path = tmp_path / "repository1"
    repo_path.mkdir()
    repo = git.Repo.init(repo_path)

    # Configure git user
    repo.config_writer().set_value("user", "name", "Test User").release()
    repo.config_writer().set_value("user", "email", "test@example.com").release()

    # Create and checkout master branch
    repo.git.checkout("-b", "master")

    # Create initial commit
    (repo_path / "README.md").write_text("# Test Repository")
    repo.index.add(["README.md"])
    repo.index.commit("Initial commit")

    # Create test files
    py_content = """import os
import sys
import json
def main():
    print('Hello, World!')
    return True
def helper():
    return True
if __name__ == '__main__':
    main()"""
    (repo_path / "test.py").write_text(py_content)
    (repo_path / "test.js").write_text("console.log('Hello, World!');")
    repo.index.add(["test.py", "test.js"])
    repo.index.commit("Add test files")

    return repo_path


# Remote repository tests
class TestRemoteProperties:
    @pytest.mark.remote
    def test_repo_name(self, remote_repo):
        assert remote_repo.repo_name == "git-pandas"

    @pytest.mark.remote
    def test_branches(self, remote_repo):
        branches = list(remote_repo.branches()["branch"].values)
        assert "master" in branches
        assert "gh-pages" in branches

    @pytest.mark.remote
    def test_tags(self, remote_repo):
        tags = list(remote_repo.tags()["tag"].values)
        assert "0.0.1" in tags
        assert "0.0.2" in tags

    @pytest.mark.remote
    def test_is_bare(self, remote_repo):
        assert not remote_repo.is_bare()


# Local repository tests
class TestLocalProperties:
    def test_repo_name(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        assert repo.repo_name == "repository1"

    def test_branches(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        branches = list(repo.branches()["branch"].values)
        assert "master" in branches

    def test_tags(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        tags = repo.tags()
        assert len(tags) == 0

    def test_is_bare(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        assert not repo.is_bare()

    def test_commit_history(self, local_repo):
        """Test commit history retrieval."""
        repo = Repository(working_dir=str(local_repo))
        history = repo.commit_history(branch="master")
        assert isinstance(history, DataFrame)
        assert "repository" in history.columns
        assert len(history) > 0

    def test_file_change_history(self, local_repo):
        """Test file change history retrieval."""
        repo = Repository(working_dir=str(local_repo))
        history = repo.file_change_history(branch="master")
        assert isinstance(history, DataFrame)
        assert "repository" in history.columns
        assert len(history) > 0

    def test_file_change_rates(self, local_repo):
        """Test file change rates calculation."""
        repo = Repository(working_dir=str(local_repo))
        rates = repo.file_change_rates(branch="master")
        assert isinstance(rates, DataFrame)
        assert "repository" in rates.columns
        assert len(rates) > 0

    def test_has_coverage(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        # We know this repo doesn't have coverage
        assert not repo.has_coverage()

    def test_bus_factor(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        # We know this repo only has one committer
        assert repo.bus_factor(by="repository")["bus factor"].values[0] == 1

    def test_blame(self, local_repo):
        repo = Repository(working_dir=str(local_repo))
        blame = repo.blame(ignore_globs=["*.[!p][!y]"])
        assert blame["loc"].sum() == 10
        assert blame.shape[0] == 1

    def test_cumulative_blame(self, local_repo):
        """Test cumulative blame calculation."""
        repo = Repository(working_dir=str(local_repo))
        blame = repo.cumulative_blame(branch="master")
        assert isinstance(blame, DataFrame)
        assert len(blame) > 0

    def test_revs(self, local_repo):
        """Test revision history retrieval."""
        repo = Repository(working_dir=str(local_repo))
        revs = repo.revs(branch="master")
        assert isinstance(revs, DataFrame)
        assert "repository" in revs.columns
        assert len(revs) > 0
