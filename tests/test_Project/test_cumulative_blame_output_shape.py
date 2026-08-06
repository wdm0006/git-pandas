"""Regression tests for the shape of ``ProjectDirectory.cumulative_blame``.

The project-level frame inherited the reverse-chronological index from
``Repository.cumulative_blame``, which broke label-based date slicing and negated
``.diff()``.
"""

import datetime

import git
import pytest

from gitpandas import ProjectDirectory
from gitpandas.cache import EphemeralCache
from tests.test_Repository.test_cumulative_blame_output_shape import (
    ALICE,
    BOB,
    DATES,
    EXPECTED_ALICE,
    EXPECTED_BOB,
    EXPECTED_TOTAL,
    build_blame_repo,
)

# ProjectDirectory.cumulative_blame(by='committer') lowercases the contributor names
PROJECT_COLUMNS = sorted([ALICE.lower(), BOB.lower()])


@pytest.fixture
def repo_dir(tmp_path, default_branch):
    return build_blame_repo(tmp_path / "blame_repo", default_branch)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def project(request, repo_dir, default_branch):
    cache_backend = EphemeralCache() if request.param else None
    return ProjectDirectory(
        working_dir=[str(repo_dir)],
        default_branch=default_branch,
        cache_backend=cache_backend,
        verbose=False,
    )


@pytest.fixture
def blame_frame(project, default_branch):
    return project.cumulative_blame(branch=default_branch)


class TestProjectCumulativeBlameOutputShape:
    def test_columns_are_contributors_only(self, blame_frame):
        assert sorted(blame_frame.columns) == PROJECT_COLUMNS

    def test_index_is_ascending(self, blame_frame):
        assert blame_frame.index.is_monotonic_increasing
        assert blame_frame.index.tolist() == DATES

    def test_exact_loc_progression(self, blame_frame):
        assert blame_frame[ALICE.lower()].tolist() == [float(x) for x in EXPECTED_ALICE]
        assert blame_frame[BOB.lower()].tolist() == [float(x) for x in EXPECTED_BOB]

    def test_sum_axis_1_gives_total_loc(self, blame_frame):
        assert blame_frame.sum(axis=1).tolist() == [float(x) for x in EXPECTED_TOTAL]

    def test_date_slicing_works(self, blame_frame):
        window = blame_frame.loc["2023-11-17":"2023-11-18"]

        assert window.index.tolist() == DATES[1:3]
        assert window.sum(axis=1).tolist() == [float(x) for x in EXPECTED_TOTAL[1:3]]

    def test_matches_single_repo_column_semantics(self, project, blame_frame, default_branch):
        """The project frame carries the same contributors as the per-repo frames."""
        for repo in project.repos:
            single = repo.cumulative_blame(branch=default_branch)

            assert sorted(c.lower() for c in single.columns) == sorted(blame_frame.columns)
            assert single.index.tolist() == blame_frame.index.tolist()


def _build_staggered_repo(repo_dir, default_branch, name, steps):
    """Build a repo whose revisions land on the given day offsets with the given line counts."""
    repo_dir.mkdir(parents=True)
    repo = git.Repo.init(str(repo_dir))
    repo.git.config("user.name", "Fallback User")
    repo.git.config("user.email", "fallback@example.com")
    repo.git.checkout("-b", default_branch)

    email = f"{name.split()[0].lower()}@example.com"
    for day_offset, lines in steps:
        (repo_dir / "src.txt").write_text("".join(f"line {n}\n" for n in range(1, lines + 1)))
        stamp = f"{int((DATES[0] + datetime.timedelta(days=day_offset)).timestamp())} +0000"
        repo.git.update_environment(
            GIT_COMMITTER_NAME=name,
            GIT_COMMITTER_EMAIL=email,
            GIT_COMMITTER_DATE=stamp,
            GIT_AUTHOR_DATE=stamp,
        )
        repo.git.add(all=True)
        repo.git.commit(m=f"{name} -> {lines}", author=f"{name} <{email}>", date=stamp)

    return repo_dir


class TestMultiRepoCumulativeBlame:
    """Two repos with revisions at different timestamps, so the forward fill is exercised."""

    @pytest.fixture
    def staggered_project(self, tmp_path, default_branch):
        _build_staggered_repo(tmp_path / "alpha", default_branch, ALICE, [(0, 2), (2, 4)])
        _build_staggered_repo(tmp_path / "beta", default_branch, BOB, [(1, 5)])

        return ProjectDirectory(
            working_dir=[str(tmp_path / "alpha"), str(tmp_path / "beta")],
            default_branch=default_branch,
            verbose=False,
        )

    def test_forward_fill_runs_chronologically(self, staggered_project, default_branch):
        frame = staggered_project.cumulative_blame(branch=default_branch)

        assert frame.index.is_monotonic_increasing
        assert frame.index.tolist() == DATES[:3]
        # alice has no revision on day 2, so her day-1 count carries forward, not backward
        assert frame[ALICE.lower()].tolist() == [2.0, 2.0, 4.0]
        # bob's repo starts on day 2, so day 1 is a zero-filled gap
        assert frame[BOB.lower()].tolist() == [0.0, 5.0, 5.0]
        assert frame.sum(axis=1).tolist() == [2.0, 7.0, 9.0]
