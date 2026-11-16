"""ProjectDirectory.knowledge_risk: repository-local identity, one evaluation instant, error contract."""

from unittest.mock import patch

import pandas as pd
import pytest

from gitpandas import GitHubProfile, ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache
from tests.test_cache_parity import _git

DAY = 86400
T0 = 1_700_000_000
COLUMNS = [
    "repository",
    "file",
    "owner",
    "owner_loc",
    "total_loc",
    "owner_share",
    "owner_last_commit",
    "days_inactive",
    "at_risk",
]
# (committer/author, epoch, {file: lines}). "Alice" owns shared.txt in both repositories, but her last
# commit is T0 in "old" (main) and T0+150d in "new" (master); only "old" has Bob committing after her.
REPOS = {
    "old": ("main", [("Alice", T0, {"shared.txt": 4, "old_only.txt": 2}), ("Bob", T0 + 10 * DAY, {"bob.txt": 3})]),
    "new": ("master", [("Bob", T0, {"bob.txt": 3}), ("Alice", T0 + 150 * DAY, {"shared.txt": 5})]),
}
AS_OF = pd.Timestamp(T0 + 200 * DAY, unit="s", tz="UTC")


def _build(path, branch, commits):
    path.mkdir()
    _git(path, "init", "-b", branch)
    for who, epoch, files in commits:
        for name, count in files.items():
            (path / name).write_text("".join(f"{name} {who} {n}\n" for n in range(count)))
        _git(path, "add", "-A")
        env = {
            f"GIT_{role}_{field}": value
            for role in ("COMMITTER", "AUTHOR")
            for field, value in (("NAME", who), ("EMAIL", f"{who}@x.com"), ("DATE", f"{epoch} +0000"))
        }
        _git(path, "commit", "-m", f"c{epoch}", env=env)


@pytest.fixture(scope="module")
def dirs(tmp_path_factory):
    root = tmp_path_factory.mktemp("pkr")
    out = []
    for name, (branch, commits) in REPOS.items():
        _build(root / name, branch, commits)
        out.append(str(root / name))
    return out


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def cache(request):
    return EphemeralCache() if request.param else None


@pytest.fixture
def project(dirs, cache):
    return ProjectDirectory(dirs, verbose=False, cache_backend=cache)


def _expected(project, **kwargs):
    parts = []
    for repo in project.repos:
        df = repo.knowledge_risk(as_of=AS_OF, **kwargs).copy()
        df.insert(0, "repository", repo.repo_name)
        parts.append(df)
    return pd.concat(parts, ignore_index=True)


def test_matches_individual_repository_calls_exactly(project):
    got = project.knowledge_risk(as_of=AS_OF, inactive_days=100)
    pd.testing.assert_frame_equal(got, _expected(project, inactive_days=100))
    assert list(got.columns) == COLUMNS
    assert isinstance(got.index, pd.RangeIndex)
    assert got["repository"].tolist() == ["old"] * 3 + ["new"] * 2


def test_shared_filename_and_owner_stay_repository_local(project):
    got = project.knowledge_risk(as_of=AS_OF, inactive_days=100).set_index(["repository", "file"])
    old = got.loc[("old", "shared.txt")]
    new = got.loc[("new", "shared.txt")]
    assert old["owner"] == new["owner"] == "Alice"
    assert (old["owner_loc"], old["total_loc"]) == (4, 4)
    assert (new["owner_loc"], new["total_loc"]) == (5, 5)
    assert old["owner_last_commit"] == pd.Timestamp(T0, unit="s", tz="UTC")
    assert new["owner_last_commit"] == pd.Timestamp(T0 + 150 * DAY, unit="s", tz="UTC")
    assert old["days_inactive"] == pytest.approx(200.0)
    assert new["days_inactive"] == pytest.approx(50.0)
    assert bool(old["at_risk"]) is True
    assert bool(new["at_risk"]) is False


def test_mixed_main_master_default_branches_resolved_per_repository(project):
    assert {r.repo_name: r.default_branch for r in project.repos} == {"old": "main", "new": "master"}
    got = project.knowledge_risk(as_of=AS_OF)
    assert got["owner_last_commit"].notna().all()


def test_default_as_of_resolved_once_and_args_forwarded(dirs):
    project = ProjectDirectory(dirs, verbose=False)
    seen = []

    def spy(self, **kwargs):
        seen.append(kwargs)
        return pd.DataFrame(columns=COLUMNS[1:])

    with patch.object(Repository, "knowledge_risk", spy):
        project.knowledge_risk(
            rev="HEAD", inactive_days=7, committer=False, ignore_globs=["*.md"], include_globs=["*.txt"], branch="x"
        )
    assert len(seen) == 2
    assert seen[0]["as_of"] == seen[1]["as_of"]
    assert seen[0]["as_of"].tzinfo is not None
    for kwargs in seen:
        assert {k: v for k, v in kwargs.items() if k != "as_of"} == {
            "rev": "HEAD",
            "inactive_days": 7,
            "committer": False,
            "ignore_globs": ["*.md"],
            "include_globs": ["*.txt"],
            "branch": "x",
        }


def test_branch_omitted_is_forwarded_as_none(dirs):
    project = ProjectDirectory(dirs, verbose=False)
    seen = []
    with patch.object(Repository, "knowledge_risk", lambda self, **kw: seen.append(kw) or pd.DataFrame()):
        project.knowledge_risk()
    assert [kw["branch"] for kw in seen] == [None, None]


@pytest.mark.parametrize("bad", [-1, 1.5, True, "30", None])
def test_invalid_inactive_days_fails_even_on_empty_project(bad):
    with pytest.raises(ValueError, match="inactive_days"):
        ProjectDirectory([], verbose=False).knowledge_risk(inactive_days=bad)


def test_invalid_as_of_fails_even_on_empty_project():
    with pytest.raises(ValueError):
        ProjectDirectory([], verbose=False).knowledge_risk(as_of=pd.NaT)


def test_invalid_arguments_fail_before_any_member_is_visited(project):
    with patch.object(Repository, "knowledge_risk", side_effect=AssertionError("visited")), pytest.raises(ValueError):
        project.knowledge_risk(inactive_days=-1)


def test_empty_project_and_all_empty_members_share_column_contract(tmp_path, default_branch):
    empty = ProjectDirectory([], verbose=False).knowledge_risk(as_of=AS_OF)
    assert list(empty.columns) == COLUMNS
    assert empty.empty

    bare = tmp_path / "bare"
    bare.mkdir()
    _git(bare, "init", "-b", default_branch)
    all_empty = ProjectDirectory([str(bare)], verbose=False, default_branch=default_branch).knowledge_risk(as_of=AS_OF)
    assert list(all_empty.columns) == COLUMNS
    assert all_empty.empty


def test_member_errors_propagate(project):
    with patch.object(Repository, "knowledge_risk", side_effect=RuntimeError("boom")), pytest.raises(
        RuntimeError, match="boom"
    ):
        project.knowledge_risk(as_of=AS_OF)


def test_cached_equals_uncached(dirs):
    plain = ProjectDirectory(dirs, verbose=False).knowledge_risk(as_of=AS_OF)
    cached = ProjectDirectory(dirs, verbose=False, cache_backend=EphemeralCache())
    pd.testing.assert_frame_equal(cached.knowledge_risk(as_of=AS_OF), plain)
    pd.testing.assert_frame_equal(cached.knowledge_risk(as_of=AS_OF), plain)


def test_aggregation_does_not_mutate_cached_member_frames(dirs):
    backend = EphemeralCache()
    project = ProjectDirectory(dirs, verbose=False, cache_backend=backend)
    for repo in project.repos:
        repo.knowledge_risk(as_of=AS_OF)
    snapshot = {k: v.data.copy() for k, v in backend._cache.items() if isinstance(v.data, pd.DataFrame)}
    result = project.knowledge_risk(as_of=AS_OF)
    result["owner"] = "mutated"
    result["repository"] = "mutated"
    for key, before in snapshot.items():
        after = backend._cache[key].data
        assert list(after.columns) == list(before.columns)
        pd.testing.assert_frame_equal(after, before)
    assert "repository" not in project.repos[0].knowledge_risk(as_of=AS_OF).columns


def test_github_profile_inherits_method():
    assert GitHubProfile.knowledge_risk is ProjectDirectory.knowledge_risk
