"""``date_source`` lets ``commit_history`` / ``hours_estimate`` time commits by author date.

The fixture mimics a rebase: every commit carries the same committer timestamp while the
author timestamps keep their original spacing.
"""

import datetime

import git
import pandas as pd
import pytest

from gitpandas import Repository
from gitpandas.cache import EphemeralCache

BASE = datetime.datetime(2023, 11, 16, tzinfo=datetime.timezone.utc)
REBASE_EPOCH = int((BASE + datetime.timedelta(days=30)).timestamp())

# (author, minutes after BASE of the author date)
LAYOUT_A = [("Alice", 0), ("Alice", 120), ("Alice", 240), ("Bob", 600), ("Bob", 610)]
LAYOUT_B = [("Carol", 0), ("Carol", 180), ("Carol", 360), ("Dan", 600), ("Dan", 612)]

# Alice: 0.5 first-commit allowance + two gaps beyond the window (0.5 each) = 1.5; Bob: 0.5 + 10/60.
EXPECTED_A = {"Alice": 1.5, "Bob": 0.5 + 10 / 60}
EXPECTED_B = {"Carol": 1.5, "Dan": 0.5 + 12 / 60}
COLLAPSED = 0.5  # identical committer timestamps: only the single-commit allowance remains


def build_rebased_repo(repo_dir, default_branch, layout):
    repo_dir.mkdir(parents=True)
    repo = git.Repo.init(str(repo_dir))
    repo.git.config("user.name", "Rebaser")
    repo.git.config("user.email", "rebaser@example.com")
    repo.git.checkout("-b", default_branch)
    target = repo_dir / "tracked.txt"
    for index, (author, minutes) in enumerate(layout, start=1):
        target.write_text("".join(f"line {n}\n" for n in range(1, index + 1)))
        repo.git.add(all=True)
        author_stamp = f"{int((BASE + datetime.timedelta(minutes=minutes)).timestamp())} +0000"
        repo.git.update_environment(
            GIT_COMMITTER_NAME="Rebaser",
            GIT_COMMITTER_EMAIL="rebaser@example.com",
            GIT_COMMITTER_DATE=f"{REBASE_EPOCH} +0000",
            GIT_AUTHOR_DATE=author_stamp,
        )
        repo.git.commit(m=f"commit {index}", author=f"{author} <{author.lower()}@example.com>", date=author_stamp)
    return repo_dir


@pytest.fixture(scope="module")
def repo_dir(tmp_path_factory, default_branch):
    return build_rebased_repo(tmp_path_factory.mktemp("date_source") / "a", default_branch, LAYOUT_A)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def repo(request, repo_dir, default_branch):
    cache = EphemeralCache() if request.param else None
    return Repository(working_dir=str(repo_dir), cache_backend=cache, default_branch=default_branch)


def _hours(df, by):
    return dict(zip(df[by], df["hours"], strict=True))


def test_hours_by_author_date_gives_exact_hours(repo):
    df = repo.hours_estimate(committer=False, date_source="author")
    hours = _hours(df, "author")
    assert hours == pytest.approx(EXPECTED_A)


def test_hours_by_committer_date_is_collapsed(repo):
    df = repo.hours_estimate(committer=False, date_source="committer")
    assert _hours(df, "author") == pytest.approx({"Alice": COLLAPSED, "Bob": COLLAPSED})


def test_default_matches_committer(repo):
    default = repo.hours_estimate(committer=False)
    explicit = repo.hours_estimate(committer=False, date_source="committer")
    assert default.equals(explicit)


def test_commit_history_index_follows_date_source(repo):
    committer = repo.commit_history(date_source="committer")
    author = repo.commit_history(date_source="author")
    assert set(committer.index) == {pd.Timestamp(REBASE_EPOCH, unit="s", tz="UTC")}
    expected = sorted(BASE + datetime.timedelta(minutes=m) for _, m in LAYOUT_A)
    assert [ts.to_pydatetime() for ts in sorted(author.index)] == expected


def test_days_cutoff_uses_selected_date(repo):
    days = (datetime.datetime.now(datetime.timezone.utc) - BASE).days + 5
    assert len(repo.commit_history(days=days, date_source="author")) == len(LAYOUT_A)
    assert len(repo.commit_history(days=days, date_source="committer")) == len(LAYOUT_A)
    # A window that reaches back past the committer date but not the author dates separates the two.
    days_between = (datetime.datetime.now(datetime.timezone.utc) - (BASE + datetime.timedelta(days=15))).days
    assert len(repo.commit_history(days=days_between, date_source="committer")) == len(LAYOUT_A)
    assert len(repo.commit_history(days=days_between, date_source="author")) == 0


@pytest.mark.parametrize("bad", ["Author", "", None, "both", 1])
def test_invalid_date_source_raises(repo, bad):
    with pytest.raises(ValueError, match="date_source"):
        repo.commit_history(date_source=bad)
    with pytest.raises(ValueError, match="date_source"):
        repo.hours_estimate(date_source=bad)


def test_invalid_positional_date_source_raises(repo):
    with pytest.raises(ValueError, match="date_source"):
        repo.commit_history(None, None, None, None, None, "nope")


def test_cache_keys_are_distinct(repo_dir, default_branch):
    cache = EphemeralCache()
    repo = Repository(working_dir=str(repo_dir), cache_backend=cache, default_branch=default_branch)
    repo.commit_history(branch=default_branch, date_source="committer")
    repo.commit_history(branch=default_branch, date_source="author")
    keys = [k for k in cache._cache if k.startswith("commit_history||")]
    assert len(keys) == 2
    assert len({k.split("||")[-1] for k in keys}) == 2


def test_cached_matches_uncached(repo_dir, default_branch):
    plain = Repository(working_dir=str(repo_dir), default_branch=default_branch)
    cached = Repository(working_dir=str(repo_dir), cache_backend=EphemeralCache(), default_branch=default_branch)
    for source in ("committer", "author"):
        for _ in range(2):  # second call is a cache hit
            a = plain.hours_estimate(committer=False, date_source=source).sort_values("author")
            b = cached.hours_estimate(committer=False, date_source=source).sort_values("author")
            assert a.reset_index(drop=True).equals(b.reset_index(drop=True))
