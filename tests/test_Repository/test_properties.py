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
def local_repo(tmp_path, default_branch):
    """Create a local git repository for testing."""
    repo_path = tmp_path / "repository1"
    repo_path.mkdir()
    repo = git.Repo.init(repo_path)

    # Configure git user
    repo.config_writer().set_value("user", "name", "Test User").release()
    repo.config_writer().set_value("user", "email", "test@example.com").release()

    # Create and checkout default branch
    repo.git.checkout("-b", default_branch)

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
    def test_branches(self, remote_repo, default_branch):
        branches = list(remote_repo.branches()["branch"].values)
        # Check for the default branch (could be master or main)
        assert default_branch in branches or "master" in branches or "main" in branches
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
    def test_repo_name(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        assert repo.repo_name == "repository1"

    def test_branches(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        branches = list(repo.branches()["branch"].values)
        assert default_branch in branches

    def test_tags(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        tags = repo.tags()
        assert len(tags) == 0

    def test_is_bare(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        assert not repo.is_bare()

    def test_commit_history(self, local_repo, default_branch):
        """Test commit history retrieval."""
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        history = repo.commit_history(branch=default_branch)
        assert isinstance(history, DataFrame)
        assert "repository" in history.columns
        assert len(history) > 0

    def test_file_change_history(self, local_repo, default_branch):
        """Test file change history retrieval."""
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        history = repo.file_change_history(branch=default_branch)
        assert isinstance(history, DataFrame)
        assert "repository" in history.columns
        assert len(history) > 0

    def test_file_change_rates(self, local_repo, default_branch):
        """Test file change rates calculation."""
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        rates = repo.file_change_rates(branch=default_branch)
        assert isinstance(rates, DataFrame)
        assert "repository" in rates.columns
        assert len(rates) > 0

    def test_has_coverage(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        # We know this repo doesn't have coverage
        assert not repo.has_coverage()

    def test_bus_factor(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        # We know this repo only has one committer
        assert repo.bus_factor(by="repository")["bus factor"].values[0] == 1

    def test_blame(self, local_repo, default_branch):
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        blame = repo.blame(ignore_globs=["*.[!p][!y]"])
        assert blame["loc"].sum() == 10
        assert blame.shape[0] == 1

    def test_cumulative_blame(self, local_repo, default_branch):
        """Test cumulative blame calculation."""
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        blame = repo.cumulative_blame(branch=default_branch)
        assert isinstance(blame, DataFrame)
        assert len(blame) > 0

    def test_revs(self, local_repo, default_branch):
        """Test revision history retrieval."""
        repo = Repository(working_dir=str(local_repo), default_branch=default_branch)
        revs = repo.revs(branch=default_branch)
        assert isinstance(revs, DataFrame)
        assert "repository" in revs.columns
        assert len(revs) > 0
