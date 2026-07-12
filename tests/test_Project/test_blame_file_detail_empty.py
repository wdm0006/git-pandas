"""Regression tests for ProjectDirectory.blame/file_detail when every repo fails.

These methods initialize their accumulator to None and populate it only inside a
try/except GitCommandError. Before the guard was added, an all-repos-fail (or
empty project) run left the accumulator as None and crashed with
``AttributeError: 'NoneType' object has no attribute 'reset_index'``. Each method
should instead return a well-formed empty DataFrame.
"""

from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import git
import pandas as pd
import pytest
from git import GitCommandError

from gitpandas import ProjectDirectory


@contextmanager
def patch_all(repos, method, exc):
    """Patch the same method on every repo to raise the given exception."""
    with ExitStack() as stack:
        for repo in repos:
            stack.enter_context(patch.object(repo, method, side_effect=exc))
        yield


class TestBlameFileDetailAllReposFail:
    @pytest.fixture
    def temp_repos(self, tmp_path):
        """Create two temporary git repositories with an initial commit each."""
        repos = []
        for i in range(2):
            repo_path = tmp_path / f"test_repo_{i}"
            repo_path.mkdir()
            repo = git.Repo.init(repo_path)
            repo.config_writer().set_value("user", "name", "Test User").release()
            repo.config_writer().set_value("user", "email", "test@example.com").release()
            (repo_path / "README.md").write_text(f"# Test Repository {i}")
            repo.index.add(["README.md"])
            repo.index.commit(f"Initial commit for repo {i}")
            repos.append(str(repo_path))
        return repos

    def test_blame_all_repos_fail(self, temp_repos):
        """blame() returns an empty DataFrame when every repo raises GitCommandError."""
        project = ProjectDirectory(working_dir=temp_repos)

        with patch_all(project.repos, "blame", GitCommandError("blame failed", 128)):
            blame_df = project.blame()

        assert isinstance(blame_df, pd.DataFrame)
        assert blame_df.empty
        assert list(blame_df.columns) == ["committer", "loc"]

    def test_blame_by_file_all_repos_fail(self, temp_repos):
        """blame(by='file') returns an empty DataFrame with the file column."""
        project = ProjectDirectory(working_dir=temp_repos)

        with patch_all(project.repos, "blame", GitCommandError("blame failed", 128)):
            blame_df = project.blame(by="file")

        assert isinstance(blame_df, pd.DataFrame)
        assert blame_df.empty
        assert list(blame_df.columns) == ["committer", "file", "loc"]

    def test_file_detail_all_repos_fail(self, temp_repos):
        """file_detail() returns an empty DataFrame when every repo raises GitCommandError."""
        project = ProjectDirectory(working_dir=temp_repos)

        with patch_all(project.repos, "file_detail", GitCommandError("file_detail failed", 128)):
            detail_df = project.file_detail()

        assert isinstance(detail_df, pd.DataFrame)
        assert detail_df.empty
        assert list(detail_df.index.names) == ["file", "repository"]


class TestBlameFileDetailEmptyProject:
    def test_blame_empty_project(self):
        """blame() on a project with no repos returns an empty DataFrame."""
        project = ProjectDirectory(working_dir=[])
        assert len(project.repos) == 0

        blame_df = project.blame()
        assert isinstance(blame_df, pd.DataFrame)
        assert blame_df.empty
        assert list(blame_df.columns) == ["committer", "loc"]

    def test_file_detail_empty_project(self):
        """file_detail() on a project with no repos returns an empty DataFrame."""
        project = ProjectDirectory(working_dir=[])
        assert len(project.repos) == 0

        detail_df = project.file_detail()
        assert isinstance(detail_df, pd.DataFrame)
        assert detail_df.empty
        assert list(detail_df.index.names) == ["file", "repository"]
