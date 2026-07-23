"""
Tests that downstream methods never mutate DataFrames held in a cache backend.

Cache backends hand out the stored DataFrame by reference, so any in-place write on a
cached result rewrites what every later cache hit returns.
"""

import git
import pandas as pd
import pytest

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache

__author__ = "willmcginnis"


def _build_repo(repo_dir, default_branch):
    repo_dir.mkdir()
    grepo = git.Repo.init(str(repo_dir))
    grepo.git.config("user.name", "Test User")
    grepo.git.config("user.email", "test@example.com")
    grepo.git.checkout("-b", default_branch)

    for idx, (day, hour) in enumerate([(2, 9), (3, 14), (4, 19), (5, 22)]):
        (repo_dir / f"file_{idx}.py").write_text("import sys\nimport os\n")
        grepo.git.add(all=True)
        env = {
            "GIT_AUTHOR_DATE": f"2023-01-{day:02d}T{hour:02d}:00:00",
            "GIT_COMMITTER_DATE": f"2023-01-{day:02d}T{hour:02d}:00:00",
        }
        grepo.git.commit(m=f"adding file_{idx}.py", env=env)

    return grepo


def _snapshot(cache):
    return {
        key: (list(entry.data.columns), entry.data.shape, entry.data.copy())
        for key, entry in cache._cache.items()
        if isinstance(entry.data, pd.DataFrame)
    }


def _assert_unchanged(cache, snapshot):
    for key, (columns, shape, frame) in snapshot.items():
        current = cache._cache[key].data
        assert list(current.columns) == columns, f"columns of cached entry {key} changed"
        assert current.shape == shape, f"shape of cached entry {key} changed"
        assert current.equals(frame), f"values of cached entry {key} changed"


@pytest.fixture
def cached_repo(tmp_path, default_branch):
    _build_repo(tmp_path / "repository1", default_branch)
    repo = Repository(
        working_dir=str(tmp_path / "repository1"),
        default_branch=default_branch,
        cache_backend=EphemeralCache(),
    )
    yield repo
    repo.__del__()


@pytest.fixture
def cached_project(tmp_path, default_branch):
    paths = []
    for name in ("repository1", "repository2"):
        _build_repo(tmp_path / name, default_branch)
        paths.append(str(tmp_path / name))

    project = ProjectDirectory(
        working_dir=paths,
        verbose=False,
        default_branch=default_branch,
        cache_backend=EphemeralCache(),
    )
    yield project
    project.__del__()


class TestCacheImmutability:
    def test_punchcard_does_not_mutate_cached_commit_history(self, cached_repo, default_branch):
        """punchcard() adds day_of_week/hour_of_day columns; they must not leak into the cache."""
        before = cached_repo.commit_history(branch=default_branch)
        columns, shape = list(before.columns), before.shape
        snapshot = _snapshot(cached_repo.cache_backend)
        assert snapshot, "expected commit_history to be cached"

        punchcard = cached_repo.punchcard(branch=default_branch)

        # punchcard still returns what it always did
        for col in ("hour_of_day", "day_of_week", "lines", "insertions", "deletions", "net"):
            assert col in punchcard.columns
        assert not punchcard.empty

        after = cached_repo.commit_history(branch=default_branch)
        assert list(after.columns) == columns
        assert after.shape == shape
        assert "day_of_week" not in after.columns
        assert "hour_of_day" not in after.columns
        assert after.equals(before)
        _assert_unchanged(cached_repo.cache_backend, snapshot)

    def test_punchcard_by_does_not_mutate_cached_commit_history(self, cached_repo, default_branch):
        """The by= aggregation path shares the same commit_history frame."""
        cached_repo.commit_history(branch=default_branch)
        snapshot = _snapshot(cached_repo.cache_backend)

        punchcard = cached_repo.punchcard(branch=default_branch, by="committer")
        assert "committer" in punchcard.columns

        _assert_unchanged(cached_repo.cache_backend, snapshot)

    def test_project_methods_do_not_mutate_cached_repo_frames(self, cached_project, default_branch):
        """ProjectDirectory aggregation must leave each member repo's cached frames alone."""
        for repo in cached_project.repos:
            repo.commit_history(branch=default_branch)
            repo.file_detail()
            repo.revs(branch=default_branch)

        snapshots = {repo.repo_name: _snapshot(repo.cache_backend) for repo in cached_project.repos}
        assert all(snapshots.values()), "expected per-repo entries to be cached"

        assert not cached_project.punchcard(branch=default_branch).empty
        assert not cached_project.commit_history(branch=default_branch).empty
        assert not cached_project.file_detail().empty
        assert not cached_project.revs(branch=default_branch).empty

        for repo in cached_project.repos:
            _assert_unchanged(repo.cache_backend, snapshots[repo.repo_name])
