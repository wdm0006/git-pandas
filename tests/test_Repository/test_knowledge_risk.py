"""Exact-value coverage for Repository.knowledge_risk on a pinned two-author fixture."""

import pandas as pd
import pytest

from gitpandas import Repository
from gitpandas.cache import EphemeralCache
from tests.test_cache_parity import _git

DAY = 86400
T0 = 1_700_000_000
# Alice stops early; Bob keeps committing. Author != committer on the last commit so a swapped identity fails.
#   a.txt: Alice 4 lines (exclusive)          shared.txt: Alice 3 + Bob 3 (tie -> alphabetical => Alice)
#   b.txt: Bob 2 lines (exclusive)            mixed.txt: author Alice, committer Bob (5 lines)
COMMITS = [
    ("Alice", "Alice", T0, {"a.txt": 4, "shared.txt": 3}),
    ("Bob", "Bob", T0 + 10 * DAY, {"b.txt": 2}),
    ("Bob", "Bob", T0 + 20 * DAY, {"shared.txt": 6}),
    ("Bob", "Alice", T0 + 100 * DAY, {"mixed.txt": 5}),
]
ALICE_LAST_COMMITTER = T0
BOB_LAST_COMMITTER = T0 + 100 * DAY
ALICE_LAST_AUTHOR = T0 + 100 * DAY
BOB_LAST_AUTHOR = T0 + 20 * DAY


@pytest.fixture(scope="module")
def repo_dir(tmp_path_factory, default_branch):
    d = tmp_path_factory.mktemp("kr")
    _git(d, "init", "-b", default_branch)
    shared = []
    for committer, author, epoch, files in COMMITS:
        for name, count in files.items():
            if name == "shared.txt" and shared:
                # Bob appends 3 more lines to Alice's 3
                (d / name).write_text("".join(f"{x}\n" for x in shared) + "".join(f"bob {n}\n" for n in range(3)))
            else:
                (d / name).write_text("".join(f"{name} {n}\n" for n in range(count)))
                if name == "shared.txt":
                    shared = [f"{name} {n}" for n in range(count)]
        _git(d, "add", "-A")
        env = {
            "GIT_COMMITTER_NAME": committer,
            "GIT_COMMITTER_EMAIL": f"{committer}@x.com",
            "GIT_COMMITTER_DATE": f"{epoch} +0000",
            "GIT_AUTHOR_NAME": author,
            "GIT_AUTHOR_EMAIL": f"{author}@x.com",
            "GIT_AUTHOR_DATE": f"{epoch} +0000",
        }
        _git(d, "commit", "-m", f"c{epoch}", env=env)
        if epoch == T0:
            _git(d, "branch", "early")  # only Alice has commits here
    return d


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def repo(request, repo_dir, default_branch):
    return Repository(
        str(repo_dir), default_branch=default_branch, cache_backend=EphemeralCache() if request.param else None
    )


def _ts(epoch):
    return pd.Timestamp(epoch, unit="s", tz="UTC")


def test_committer_identity_exact_values(repo):
    as_of = _ts(T0 + 190 * DAY)
    df = repo.knowledge_risk(inactive_days=180, as_of=as_of, branch=repo.default_branch).set_index("file")

    assert list(df.index) == ["a.txt", "b.txt", "mixed.txt", "shared.txt"]
    assert df["owner"].to_dict() == {"a.txt": "Alice", "b.txt": "Bob", "mixed.txt": "Bob", "shared.txt": "Alice"}
    assert df["owner_loc"].to_dict() == {"a.txt": 4, "b.txt": 2, "mixed.txt": 5, "shared.txt": 3}
    assert df["total_loc"].to_dict() == {"a.txt": 4, "b.txt": 2, "mixed.txt": 5, "shared.txt": 6}
    assert df["owner_share"].to_dict() == {"a.txt": 1.0, "b.txt": 1.0, "mixed.txt": 1.0, "shared.txt": 0.5}
    assert df.loc["a.txt", "owner_last_commit"] == _ts(ALICE_LAST_COMMITTER)
    assert df.loc["b.txt", "owner_last_commit"] == _ts(BOB_LAST_COMMITTER)
    assert df.loc["a.txt", "days_inactive"] == pytest.approx(190.0)
    assert df.loc["b.txt", "days_inactive"] == pytest.approx(90.0)
    assert df["at_risk"].to_dict() == {"a.txt": True, "b.txt": False, "mixed.txt": False, "shared.txt": True}
    assert df["at_risk"].dtype == bool


@pytest.mark.parametrize(
    ("inactive_days", "expected"), [(189, True), (190, True), (191, False)], ids=["below", "equal", "above"]
)
def test_threshold_boundary_is_inclusive(repo, inactive_days, expected):
    df = repo.knowledge_risk(
        inactive_days=inactive_days, as_of=_ts(T0 + 190 * DAY), branch=repo.default_branch
    ).set_index("file")
    assert df.loc["a.txt", "days_inactive"] == pytest.approx(190.0)
    assert bool(df.loc["a.txt", "at_risk"]) is expected


def test_author_identity_differs_from_committer(repo):
    df = repo.knowledge_risk(committer=False, as_of=_ts(T0 + 190 * DAY), branch=repo.default_branch).set_index("file")
    assert df.loc["mixed.txt", "owner"] == "Alice"
    assert df.loc["mixed.txt", "owner_last_commit"] == _ts(ALICE_LAST_AUTHOR)
    assert df.loc["b.txt", "owner_last_commit"] == _ts(BOB_LAST_AUTHOR)
    assert df.loc["a.txt", "days_inactive"] == pytest.approx(90.0)
    assert df.loc["b.txt", "days_inactive"] == pytest.approx(170.0)


def test_naive_as_of_is_utc_and_default_is_now(repo):
    naive = repo.knowledge_risk(as_of=pd.Timestamp(T0 + 190 * DAY, unit="s"), branch=repo.default_branch)
    aware = repo.knowledge_risk(as_of=_ts(T0 + 190 * DAY), branch=repo.default_branch)
    pd.testing.assert_frame_equal(naive, aware)
    now = repo.knowledge_risk(branch=repo.default_branch)
    assert (now["days_inactive"] > 365).all()
    assert now["at_risk"].all()


def test_cached_result_is_not_stale_across_as_of(repo):
    first = repo.knowledge_risk(as_of=_ts(T0 + 190 * DAY), branch=repo.default_branch)
    second = repo.knowledge_risk(as_of=_ts(T0 + 200 * DAY), branch=repo.default_branch)
    assert second.set_index("file").loc["a.txt", "days_inactive"] == pytest.approx(200.0)
    assert first.set_index("file").loc["a.txt", "days_inactive"] == pytest.approx(190.0)


def test_owner_without_commits_on_branch_is_not_flagged(repo_dir, default_branch):
    repo = Repository(str(repo_dir), default_branch=default_branch)
    df = repo.knowledge_risk(branch="early", as_of=_ts(T0 + 190 * DAY))
    df = df.set_index("file")
    # Bob owns b.txt/mixed.txt but has no commits on "early"; Alice does.
    assert df.loc[["b.txt", "mixed.txt"], "owner_last_commit"].isna().all()
    assert df.loc[["b.txt", "mixed.txt"], "days_inactive"].isna().all()
    assert not df.loc[["b.txt", "mixed.txt"], "at_risk"].any()
    assert df.loc["a.txt", "days_inactive"] == pytest.approx(190.0)
    assert df.loc["a.txt", "at_risk"]
    assert df["at_risk"].dtype == bool


def test_globs_filter_files(repo):
    df = repo.knowledge_risk(include_globs=["a.*"], as_of=_ts(T0 + 190 * DAY), branch=repo.default_branch)
    assert list(df["file"]) == ["a.txt"]


def test_empty_repository_keeps_column_order(tmp_path, default_branch):
    _git(tmp_path, "init", "-b", default_branch)
    df = Repository(str(tmp_path), default_branch=default_branch).knowledge_risk(as_of=_ts(T0))
    assert len(df) == 0
    assert list(df.columns) == [
        "file", "owner", "owner_loc", "total_loc", "owner_share", "owner_last_commit", "days_inactive", "at_risk",
    ]  # fmt: skip


@pytest.mark.parametrize("bad", [-1, True, False, 1.5, 180.0, "180", None])
def test_invalid_inactive_days_raises_before_git_or_cache(bad, repo_dir, default_branch):
    cache = EphemeralCache()
    repo = Repository(str(repo_dir), default_branch=default_branch, cache_backend=cache)
    before = set(cache._cache)
    with pytest.raises(ValueError, match="inactive_days"):
        repo.knowledge_risk(inactive_days=bad)
    with pytest.raises(ValueError, match="inactive_days"):
        repo.knowledge_risk("HEAD", bad)
    assert set(cache._cache) == before


def test_zero_inactive_days_flags_everyone(repo):
    df = repo.knowledge_risk(inactive_days=0, as_of=_ts(T0 + 190 * DAY), branch=repo.default_branch)
    assert df["at_risk"].all()
