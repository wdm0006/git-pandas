"""Negative, boolean, float and NaN ``limit`` values are rejected before Git or cache access."""

import inspect

import git
import pytest

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache

METHODS = [
    "commit_history",
    "file_change_history",
    "file_change_rates",
    "hours_estimate",
    "revs",
    "punchcard",
    "cumulative_blame",
    "parallel_cumulative_blame",
]
PROJECT_METHODS = [m for m in METHODS if m != "parallel_cumulative_blame"]
BAD_LIMITS = [-1, -5, True, False, 1.0, 2.5, float("nan"), "3"]


def _build(path, branch):
    path.mkdir()
    repo = git.Repo.init(str(path))
    repo.git.config("user.name", "T")
    repo.git.config("user.email", "t@example.com")
    repo.git.checkout("-b", branch)
    for i in range(3):
        (path / "f.txt").write_text(f"{i}\n" * (i + 1))
        repo.git.add(all=True)
        repo.git.commit(m=f"c{i}")
    return path


@pytest.fixture(scope="module")
def repo_dirs(tmp_path_factory, default_branch):
    root = tmp_path_factory.mktemp("limit_validation")
    return [_build(root / f"r{i}", default_branch) for i in range(2)]


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def cache(request):
    return EphemeralCache() if request.param else None


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("limit", BAD_LIMITS, ids=repr)
def test_repository_rejects_bad_limit(repo_dirs, default_branch, cache, method, limit):
    repo = Repository(str(repo_dirs[0]), default_branch=default_branch, cache_backend=cache)
    with pytest.raises(ValueError, match="limit must be None or a non-negative integer"):
        getattr(repo, method)(limit=limit)
    if cache is not None:
        assert cache.list_cached_keys() == []


@pytest.mark.parametrize("method", METHODS)
def test_repository_rejects_positional_and_force_refresh(repo_dirs, default_branch, cache, method):
    repo = Repository(str(repo_dirs[0]), default_branch=default_branch, cache_backend=cache)
    params = list(inspect.signature(getattr(Repository, method)).parameters)[1:]
    positional = [default_branch] + [None] * (params.index("limit") - 1) + [-1]
    with pytest.raises(ValueError, match="limit must be"):
        getattr(repo, method)(*positional)
    with pytest.raises(ValueError, match="limit must be"):
        getattr(repo, method)(limit=-1, **({"force_refresh": True} if cache is not None else {}))


@pytest.mark.parametrize("method", PROJECT_METHODS)
@pytest.mark.parametrize("limit", [-1, True, 1.5, float("nan")], ids=repr)
def test_project_rejects_bad_limit(repo_dirs, default_branch, cache, method, limit):
    project = ProjectDirectory(
        working_dir=[str(d) for d in repo_dirs], default_branch=default_branch, cache_backend=cache
    )
    with pytest.raises(ValueError, match="limit must be None or a non-negative integer"):
        getattr(project, method)(limit=limit)
    if cache is not None:
        assert cache.list_cached_keys() == []


@pytest.mark.parametrize("method", PROJECT_METHODS)
def test_empty_project_still_validates(method):
    with pytest.raises(ValueError, match="limit must be"):
        getattr(ProjectDirectory(working_dir=[]), method)(limit=-1)


def test_valid_limits_unchanged(repo_dirs, default_branch, cache):
    repo = Repository(str(repo_dirs[0]), default_branch=default_branch, cache_backend=cache)
    assert len(repo.commit_history(limit=0)) == 0
    assert len(repo.commit_history(limit=2)) == 2
    assert len(repo.commit_history()) == 3
    assert len(repo.revs(limit=0)) == 0
    project = ProjectDirectory(
        working_dir=[str(d) for d in repo_dirs], default_branch=default_branch, cache_backend=cache
    )
    assert len(project.commit_history(limit=3)) == 3
    assert len(project.commit_history(limit=0)) == 0
