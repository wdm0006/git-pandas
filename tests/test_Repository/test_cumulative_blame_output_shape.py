"""Regression tests for the shape of the cumulative blame frames.

``Repository.cumulative_blame`` and ``Repository.parallel_cumulative_blame`` used to
return the string ``repository`` label column alongside the per-contributor LOC counts,
which made ``df.sum(axis=1)`` raise ``TypeError``, and they returned a reverse
chronological index, which made ``df.loc[a:b]`` raise ``KeyError``.
"""

import datetime

import git
import pytest

from gitpandas import Repository
from gitpandas.cache import EphemeralCache

ALICE = "Alice Dev"
BOB = "Bob Dev"

# one commit per day, so every timestamp is distinct and label-sliceable
BASE = datetime.datetime(2023, 11, 16, tzinfo=datetime.timezone.utc)
DATES = [BASE + datetime.timedelta(days=offset) for offset in range(4)]

# (alice.txt lines, bob.txt lines) after each commit
EXPECTED_ALICE = [3, 3, 5, 5]
EXPECTED_BOB = [0, 2, 2, 4]
EXPECTED_TOTAL = [3, 5, 7, 9]


def _commit(repo, message, name, timestamp):
    email = f"{name.split()[0].lower()}@example.com"
    stamp = f"{int(timestamp.timestamp())} +0000"
    repo.git.update_environment(
        GIT_COMMITTER_NAME=name,
        GIT_COMMITTER_EMAIL=email,
        GIT_COMMITTER_DATE=stamp,
        GIT_AUTHOR_DATE=stamp,
    )
    repo.git.commit(m=message, author=f"{name} <{email}>", date=stamp)


def build_blame_repo(repo_dir, default_branch):
    """Build a repo with two contributors and known per-revision line counts."""
    repo_dir.mkdir(parents=True)
    repo = git.Repo.init(str(repo_dir))
    repo.git.config("user.name", "Fallback User")
    repo.git.config("user.email", "fallback@example.com")
    repo.git.checkout("-b", default_branch)

    alice_file = repo_dir / "alice.txt"
    bob_file = repo_dir / "bob.txt"

    def write(path, lines):
        path.write_text("".join(f"line {n}\n" for n in range(1, lines + 1)))

    steps = [
        (alice_file, 3, ALICE),
        (bob_file, 2, BOB),
        (alice_file, 5, ALICE),
        (bob_file, 4, BOB),
    ]
    for (path, lines, name), timestamp in zip(steps, DATES, strict=True):
        write(path, lines)
        repo.git.add(all=True)
        _commit(repo, f"{name} -> {path.name}:{lines}", name, timestamp)

    return repo_dir


@pytest.fixture
def repo_dir(tmp_path, default_branch):
    return build_blame_repo(tmp_path / "blame_repo", default_branch)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def repository(request, repo_dir, default_branch):
    cache_backend = EphemeralCache() if request.param else None
    repo = Repository(
        working_dir=str(repo_dir),
        default_branch=default_branch,
        cache_backend=cache_backend,
        labels_to_add=["team-x"],
    )
    yield repo
    repo.__del__()


@pytest.fixture(params=["cumulative_blame", "parallel_cumulative_blame"])
def blame_frame(request, repository, default_branch):
    return getattr(repository, request.param)(branch=default_branch)


class TestCumulativeBlameOutputShape:
    def test_columns_are_contributors_only(self, blame_frame):
        """No 'repository' column and no configured label columns."""
        assert sorted(blame_frame.columns) == [ALICE, BOB]

    def test_index_is_ascending(self, blame_frame):
        assert blame_frame.index.is_monotonic_increasing
        assert blame_frame.index.tolist() == DATES

    def test_exact_loc_progression(self, blame_frame):
        assert blame_frame[ALICE].tolist() == EXPECTED_ALICE
        assert blame_frame[BOB].tolist() == EXPECTED_BOB

    def test_sum_axis_1_gives_total_loc(self, blame_frame):
        assert blame_frame.sum(axis=1).tolist() == EXPECTED_TOTAL

    def test_date_slicing_works(self, blame_frame):
        window = blame_frame.loc["2023-11-17":"2023-11-18"]

        assert window.index.tolist() == DATES[1:3]
        assert window[ALICE].tolist() == EXPECTED_ALICE[1:3]
        assert window[BOB].tolist() == EXPECTED_BOB[1:3]

    def test_diff_is_positive_growth(self, blame_frame):
        """Reverse-chronological ordering silently negated .diff()."""
        assert blame_frame.sum(axis=1).diff().dropna().tolist() == [2, 2, 2]

    def test_serial_and_parallel_agree(self, repository, default_branch):
        serial = repository.cumulative_blame(branch=default_branch)
        parallel = repository.parallel_cumulative_blame(branch=default_branch)

        assert sorted(serial.columns) == sorted(parallel.columns)
        assert serial.index.tolist() == parallel.index.tolist()
        assert serial.sum(axis=1).tolist() == parallel.sum(axis=1).tolist()
