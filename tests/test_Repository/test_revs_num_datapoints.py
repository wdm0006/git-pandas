"""Regression tests for ``Repository.revs(num_datapoints=...)`` sample sizes.

``revs`` used to derive an integer ``skip = int(commit_count / num_datapoints)`` and then
slice every ``skip``th row, so integer truncation made the result exceed the requested
size: on a 10-commit history ``num_datapoints=4`` returned 5 rows and ``num_datapoints=6``
returned all 10. ``cumulative_blame`` and ``parallel_cumulative_blame`` delegate their
``num_datapoints`` argument to ``revs`` and inherited the same over-sampling.
"""

import datetime

import git
import pytest

from gitpandas import Repository
from gitpandas.cache import EphemeralCache

AUTHOR = "Alice Dev"
EMAIL = "alice@example.com"

# 10 commits, one per day, so timestamps are distinct and revision identity is pinned.
# 10 is deliberately not divisible by the 4 datapoints the main test requests.
COMMIT_COUNT = 10
BASE = datetime.datetime(2023, 11, 16, tzinfo=datetime.timezone.utc)
DATES = [BASE + datetime.timedelta(days=offset) for offset in range(COMMIT_COUNT)]


def build_revs_repo(repo_dir, default_branch):
    """Build a repo with ``COMMIT_COUNT`` commits at pinned, distinct timestamps."""
    repo_dir.mkdir(parents=True)
    repo = git.Repo.init(str(repo_dir))
    repo.git.config("user.name", "Fallback User")
    repo.git.config("user.email", "fallback@example.com")
    repo.git.checkout("-b", default_branch)

    target = repo_dir / "alice.txt"
    for index, timestamp in enumerate(DATES, start=1):
        target.write_text("".join(f"line {n}\n" for n in range(1, index + 1)))
        repo.git.add(all=True)
        stamp = f"{int(timestamp.timestamp())} +0000"
        repo.git.update_environment(
            GIT_COMMITTER_NAME=AUTHOR,
            GIT_COMMITTER_EMAIL=EMAIL,
            GIT_COMMITTER_DATE=stamp,
            GIT_AUTHOR_DATE=stamp,
        )
        repo.git.commit(m=f"commit {index}", author=f"{AUTHOR} <{EMAIL}>", date=stamp)

    return repo_dir


@pytest.fixture
def repo_dir(tmp_path, default_branch):
    return build_revs_repo(tmp_path / "revs_repo", default_branch)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def repository(request, repo_dir, default_branch):
    cache_backend = EphemeralCache() if request.param else None
    repo = Repository(
        working_dir=str(repo_dir),
        default_branch=default_branch,
        cache_backend=cache_backend,
    )
    yield repo
    repo.__del__()


def _epochs(df):
    return [int(value) for value in df["date"].tolist()]


class TestRevsNumDatapoints:
    def test_full_history_is_the_baseline(self, repository):
        """Sanity check: the fixture really has COMMIT_COUNT newest-first revisions."""
        df = repository.revs()
        assert df.shape[0] == COMMIT_COUNT
        assert _epochs(df) == [int(date.timestamp()) for date in reversed(DATES)]

    @pytest.mark.parametrize("num_datapoints", list(range(1, COMMIT_COUNT + 1)))
    def test_returns_exactly_the_requested_sample_size(self, repository, num_datapoints):
        df = repository.revs(num_datapoints=num_datapoints)
        assert df.shape[0] == num_datapoints

    def test_four_datapoints_of_ten_commits_are_evenly_distributed(self, repository):
        """The headline case: 10 commits sampled to 4, newest first, endpoints included."""
        df = repository.revs(num_datapoints=4)

        assert df.shape[0] == 4
        newest_first = [int(date.timestamp()) for date in reversed(DATES)]
        # positions 0, 3, 6, 9 of the newest-first history
        assert _epochs(df) == [newest_first[0], newest_first[3], newest_first[6], newest_first[9]]

        full = repository.revs()
        assert df["rev"].tolist() == [full["rev"].tolist()[position] for position in (0, 3, 6, 9)]
        # the newest and the oldest revision are both present
        assert df["rev"].iloc[0] == full["rev"].iloc[0]
        assert df["rev"].iloc[-1] == full["rev"].iloc[-1]

    @pytest.mark.parametrize("num_datapoints", [COMMIT_COUNT, COMMIT_COUNT + 1, COMMIT_COUNT * 3])
    def test_request_at_or_beyond_history_returns_each_rev_once(self, repository, num_datapoints):
        df = repository.revs(num_datapoints=num_datapoints)

        assert df.shape[0] == COMMIT_COUNT
        assert df["rev"].tolist() == repository.revs()["rev"].tolist()
        assert not df["rev"].duplicated().any()

    def test_single_datapoint_returns_the_newest_rev(self, repository):
        df = repository.revs(num_datapoints=1)

        assert df.shape[0] == 1
        assert df["rev"].iloc[0] == repository.revs()["rev"].iloc[0]

    @pytest.mark.parametrize("num_datapoints", [0, -1, -10])
    def test_non_positive_num_datapoints_raises(self, repository, num_datapoints):
        with pytest.raises(ValueError, match="num_datapoints must be a positive integer"):
            repository.revs(num_datapoints=num_datapoints)

    def test_explicit_limit_is_unchanged(self, repository):
        df = repository.revs(limit=3)

        assert df.shape[0] == 3
        assert df["rev"].tolist() == repository.revs()["rev"].tolist()[:3]

    def test_explicit_skip_is_unchanged(self, repository):
        df = repository.revs(skip=3)

        full = repository.revs()["rev"].tolist()
        assert df["rev"].tolist() == full[0::3]

    def test_explicit_limit_and_skip_are_unchanged(self, repository):
        df = repository.revs(limit=3, skip=2)

        # limit*skip commits are walked, then every skip-th one is kept
        full = repository.revs()["rev"].tolist()
        assert df["rev"].tolist() == full[:6][0::2]

    def test_num_datapoints_is_ignored_when_limit_is_given(self, repository):
        """num_datapoints only applies when neither limit nor skip was supplied."""
        assert repository.revs(limit=5, num_datapoints=2)["rev"].tolist() == repository.revs(limit=5)["rev"].tolist()
        assert repository.revs(skip=5, num_datapoints=2)["rev"].tolist() == repository.revs(skip=5)["rev"].tolist()


class TestCumulativeBlameNumDatapoints:
    @pytest.mark.parametrize("num_datapoints", [1, 3, 4, 7])
    def test_cumulative_blame_returns_at_most_n_timestamps(self, repository, num_datapoints):
        df = repository.cumulative_blame(num_datapoints=num_datapoints)

        assert df.shape[0] == num_datapoints
        assert not df.index.duplicated().any()

    @pytest.mark.parametrize("num_datapoints", [1, 3, 4, 7])
    def test_parallel_cumulative_blame_returns_at_most_n_timestamps(self, repository, num_datapoints):
        df = repository.parallel_cumulative_blame(num_datapoints=num_datapoints)

        assert df.shape[0] == num_datapoints
        assert not df.index.duplicated().any()

    def test_cumulative_blame_samples_the_same_revs_as_revs(self, repository):
        """The sampled timestamps are exactly the ones revs() selected."""
        sampled = repository.revs(num_datapoints=4)
        blame = repository.cumulative_blame(num_datapoints=4)

        expected = sorted(int(value) for value in sampled["date"].tolist())
        assert [int(stamp.timestamp()) for stamp in blame.index] == expected

    @pytest.mark.parametrize("num_datapoints", [0, -1])
    def test_cumulative_blame_rejects_non_positive_num_datapoints(self, repository, num_datapoints):
        with pytest.raises(ValueError, match="num_datapoints must be a positive integer"):
            repository.cumulative_blame(num_datapoints=num_datapoints)
