"""Regression tests for ProjectDirectory.file_detail honouring the ``committer`` flag."""

import pytest

from gitpandas import ProjectDirectory
from gitpandas.cache import EphemeralCache
from tests.test_Repository.test_file_owner_author import _build_repo


@pytest.fixture
def repos_dir(tmp_path, default_branch):
    repos_dir = tmp_path / "repos"
    for repo_number in range(2):
        _build_repo(repos_dir / f"repo{repo_number}", default_branch)
    return repos_dir


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def project(request, repos_dir, default_branch):
    cache_backend = EphemeralCache() if request.param else None
    return ProjectDirectory(working_dir=str(repos_dir), default_branch=default_branch, cache_backend=cache_backend)


def test_project_file_detail_respects_committer_flag(project):
    committer_detail = project.file_detail(committer=True)
    author_detail = project.file_detail(committer=False)

    expected_files = {("owned.py", "repo0"), ("owned.py", "repo1")}
    assert set(committer_detail.index) == expected_files
    assert set(author_detail.index) == expected_files

    assert set(committer_detail["file_owner"]) == {"Bob Committer"}
    assert set(author_detail["file_owner"]) == {"Alice Author"}
