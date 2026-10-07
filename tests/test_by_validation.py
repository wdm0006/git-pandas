"""Grouping validation must run before Git or cache access."""

from unittest.mock import Mock

import pytest

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache
from tests.test_Repository.test_file_owner_author import _build_repo


@pytest.fixture(scope="module")
def repo_dir(tmp_path_factory):
    return _build_repo(tmp_path_factory.mktemp("grouping") / "repo", "master")


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def subjects(request, repo_dir):
    cache = EphemeralCache() if request.param else None
    repo = Repository(str(repo_dir), default_branch="master", cache_backend=cache)
    return repo, ProjectDirectory([repo], verbose=False)


@pytest.mark.parametrize(
    "target,method,allowed",
    [
        (0, "blame", ("repository", "file")),
        (0, "bus_factor", ("repository", "file")),
        (1, "blame", ("repository", "file")),
        (1, "bus_factor", ("projectd", "repository", "file")),
    ],
)
@pytest.mark.parametrize("positional", [False, True], ids=["keyword", "positional"])
def test_invalid_by_before_git_and_cache(subjects, target, method, allowed, positional, monkeypatch):
    repo, project = subjects

    class UntouchableGit:
        def __getattribute__(self, name):
            pytest.fail(f"Git touched: {name}")

    monkeypatch.setattr(repo, "repo", UntouchableGit())
    monkeypatch.setattr(Repository, "repo_name", property(lambda self: pytest.fail("Git identity touched")))
    if repo.cache_backend is not None:
        monkeypatch.setattr(repo.cache_backend, "_get_entry", Mock(side_effect=AssertionError("Cache touched")))
    subject = (repo, project)[target]
    args = {
        (0, "blame"): ("HEAD", True, "bogus"),
        (0, "bus_factor"): ("bogus",),
        (1, "blame"): (True, "bogus"),
        (1, "bus_factor"): (None, None, "bogus"),
    }
    with pytest.raises(ValueError, match="by") as error:
        if positional:
            getattr(subject, method)(*args[target, method])
        else:
            getattr(subject, method)(by="bogus")
    for value in allowed:
        assert repr(value) in str(error.value)


def test_valid_groupings(subjects):
    repo, project = subjects
    for subject in (repo, project):
        assert subject.blame(by="repository")["loc"].to_dict() == {"Bob Committer": 5, "Dave Committer": 1}
        assert subject.blame(by="file")["loc"].tolist() == [5, 1]
        for by in ("repository", "file"):
            assert subject.bus_factor(by=by)["bus factor"].tolist() == [1]
    assert project.bus_factor(by="projectd")["bus factor"].tolist() == [1]


def test_cached_force_refresh(subjects):
    repo, _ = subjects
    if repo.cache_backend is None:
        return
    for method in ("blame", "bus_factor"):
        result = getattr(repo, method)(by="repository", force_refresh=True)
        assert not result.empty
        with pytest.raises(ValueError, match="repository.*file"):
            getattr(repo, method)(by="bogus", force_refresh=True)
