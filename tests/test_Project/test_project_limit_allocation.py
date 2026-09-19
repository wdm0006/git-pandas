"""Regression tests for the project-wide ``limit`` allocation.

``ProjectDirectory.hours_estimate``, ``commit_history`` and ``file_change_history`` used to
divide a project-wide ``limit`` with ``int(limit / len(self.repo_dirs))`` and hand the same
truncated value to every repository. That dropped the remainder, silently returned nothing
whenever ``limit`` was below the repository count, divided by the unfiltered ``repo_dirs``
rather than the ``ignore_repos``-filtered ``repos``, and raised ``ZeroDivisionError`` on a
project with no repositories. They now share ``ProjectDirectory._allocate_limit`` with
``revs``, which gives the first ``limit % len(repos)`` repositories one extra commit.
"""

import datetime

import git
import pytest

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache

# Each repository has a distinct sole contributor so hours_estimate can be asserted per repo.
COMMIT_COUNT = 5
BASE = datetime.datetime(2023, 11, 16, tzinfo=datetime.timezone.utc)


def build_limit_repo(repo_dir, default_branch, author, email, commit_count=COMMIT_COUNT):
    """Build a repo whose every commit touches exactly one file, at pinned timestamps."""
    repo_dir.mkdir(parents=True)
    repo = git.Repo.init(str(repo_dir))
    repo.git.config("user.name", "Fallback User")
    repo.git.config("user.email", "fallback@example.com")
    repo.git.checkout("-b", default_branch)

    target = repo_dir / "tracked.txt"
    for index in range(1, commit_count + 1):
        target.write_text("".join(f"line {n}\n" for n in range(1, index + 1)))
        repo.git.add(all=True)
        stamp = f"{int((BASE + datetime.timedelta(days=index)).timestamp())} +0000"
        repo.git.update_environment(
            GIT_COMMITTER_NAME=author,
            GIT_COMMITTER_EMAIL=email,
            GIT_COMMITTER_DATE=stamp,
            GIT_AUTHOR_DATE=stamp,
        )
        repo.git.commit(m=f"commit {index}", author=f"{author} <{email}>", date=stamp)

    return repo_dir


def _build_project_dirs(root, default_branch, repo_count):
    return [
        build_limit_repo(root / f"limit_repo_{index}", default_branch, f"Dev {index}", f"dev{index}@example.com")
        for index in range(repo_count)
    ]


@pytest.fixture(scope="module")
def two_repo_dirs(tmp_path_factory, default_branch):
    return _build_project_dirs(tmp_path_factory.mktemp("two_repos"), default_branch, 2)


@pytest.fixture(scope="module")
def three_repo_dirs(tmp_path_factory, default_branch):
    return _build_project_dirs(tmp_path_factory.mktemp("three_repos"), default_branch, 3)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def cache_backend(request):
    return EphemeralCache() if request.param else None


def _project(paths, default_branch, cache_backend):
    return ProjectDirectory(
        working_dir=[str(path) for path in paths],
        default_branch=default_branch,
        cache_backend=cache_backend,
        verbose=False,
    )


@pytest.fixture
def two_repo_project(two_repo_dirs, default_branch, cache_backend):
    projectd = _project(two_repo_dirs, default_branch, cache_backend)
    yield projectd
    projectd.__del__()


@pytest.fixture
def three_repo_project(three_repo_dirs, default_branch, cache_backend):
    projectd = _project(three_repo_dirs, default_branch, cache_backend)
    yield projectd
    projectd.__del__()


class TestProjectCommitHistoryLimit:
    @pytest.mark.parametrize("limit", list(range(1, 2 * COMMIT_COUNT + 1)))
    def test_two_repos_return_exactly_the_requested_limit(self, two_repo_project, limit):
        assert two_repo_project.commit_history(limit=limit).shape[0] == limit

    @pytest.mark.parametrize("limit", list(range(1, 3 * COMMIT_COUNT + 1)))
    def test_three_repos_return_exactly_the_requested_limit(self, three_repo_project, limit):
        assert three_repo_project.commit_history(limit=limit).shape[0] == limit

    @pytest.mark.parametrize(("limit", "expected_counts"), [(1, [1, 0]), (3, [2, 1]), (5, [3, 2])])
    def test_remainder_goes_to_the_first_repositories(self, two_repo_project, limit, expected_counts):
        """A total-only assertion passes even if the remainder lands on the wrong repository."""
        df = two_repo_project.commit_history(limit=limit)

        for repo, expected in zip(two_repo_project.repos, expected_counts, strict=True):
            rows = df.loc[df["repository"] == repo.repo_name]
            assert rows.shape[0] == expected
            assert rows["commit_sha"].tolist() == repo.commit_history(limit=expected)["commit_sha"].tolist()

    @pytest.mark.parametrize(("limit", "expected_counts"), [(1, [1, 0, 0]), (4, [2, 1, 1]), (8, [3, 3, 2])])
    def test_remainder_goes_to_the_first_repositories_of_three(self, three_repo_project, limit, expected_counts):
        df = three_repo_project.commit_history(limit=limit)

        for repo, expected in zip(three_repo_project.repos, expected_counts, strict=True):
            assert df.loc[df["repository"] == repo.repo_name].shape[0] == expected

    def test_unlimited_output_is_unchanged(self, two_repo_project):
        df = two_repo_project.commit_history()

        assert df.shape[0] == 2 * COMMIT_COUNT
        for repo in two_repo_project.repos:
            rows = df.loc[df["repository"] == repo.repo_name]
            assert rows["commit_sha"].tolist() == repo.commit_history()["commit_sha"].tolist()


class TestProjectFileChangeHistoryLimit:
    @pytest.mark.parametrize("limit", list(range(1, 2 * COMMIT_COUNT + 1)))
    def test_two_repos_return_exactly_the_requested_limit(self, two_repo_project, limit):
        """Every commit in the fixture touches exactly one file, so rows == commits."""
        assert two_repo_project.file_change_history(limit=limit).shape[0] == limit

    @pytest.mark.parametrize("limit", list(range(1, 3 * COMMIT_COUNT + 1)))
    def test_three_repos_return_exactly_the_requested_limit(self, three_repo_project, limit):
        assert three_repo_project.file_change_history(limit=limit).shape[0] == limit

    @pytest.mark.parametrize(("limit", "expected_counts"), [(1, [1, 0]), (3, [2, 1]), (5, [3, 2])])
    def test_remainder_goes_to_the_first_repositories(self, two_repo_project, limit, expected_counts):
        df = two_repo_project.file_change_history(limit=limit)

        for repo, expected in zip(two_repo_project.repos, expected_counts, strict=True):
            rows = df.loc[df["repository"] == repo.repo_name]
            assert rows.shape[0] == expected
            assert rows["message"].tolist() == repo.file_change_history(limit=expected)["message"].tolist()

    def test_unlimited_output_is_unchanged(self, two_repo_project):
        df = two_repo_project.file_change_history()

        assert df.shape[0] == 2 * COMMIT_COUNT
        for repo in two_repo_project.repos:
            rows = df.loc[df["repository"] == repo.repo_name]
            assert rows["message"].tolist() == repo.file_change_history()["message"].tolist()


class TestProjectHoursEstimateLimit:
    def test_limit_below_the_repository_count_still_reports_a_contributor(self, two_repo_project):
        df = two_repo_project.hours_estimate(limit=1)

        assert not df.empty
        assert df["committer"].tolist() == ["Dev 0"]

    @pytest.mark.parametrize(
        ("limit", "expected_committers"),
        [(1, ["Dev 0"]), (2, ["Dev 0", "Dev 1"]), (3, ["Dev 0", "Dev 1"]), (10, ["Dev 0", "Dev 1"])],
    )
    def test_every_contributor_with_a_share_is_reported(self, two_repo_project, limit, expected_committers):
        df = two_repo_project.hours_estimate(limit=limit)

        assert sorted(df["committer"].tolist()) == expected_committers

    @pytest.mark.parametrize(
        ("limit", "expected_committers"),
        [(1, ["Dev 0"]), (2, ["Dev 0", "Dev 1"]), (3, ["Dev 0", "Dev 1", "Dev 2"])],
    )
    def test_three_repos_report_every_contributor_with_a_share(self, three_repo_project, limit, expected_committers):
        df = three_repo_project.hours_estimate(limit=limit)

        assert sorted(df["committer"].tolist()) == expected_committers

    def test_unlimited_output_is_unchanged(self, two_repo_project):
        df = two_repo_project.hours_estimate()

        assert sorted(df["committer"].tolist()) == ["Dev 0", "Dev 1"]
        for repo in two_repo_project.repos:
            rows = df.loc[df["repository"] == repo.repo_name]
            assert rows["hours"].tolist() == repo.hours_estimate()["hours"].tolist()


class TestProjectRevsLimitParity:
    @pytest.mark.parametrize("limit", list(range(1, 2 * COMMIT_COUNT + 1)))
    def test_revs_still_returns_exactly_the_requested_limit(self, two_repo_project, limit):
        """``revs`` shares the allocation helper; its #85 behaviour must not drift."""
        assert two_repo_project.revs(limit=limit).shape[0] == limit


class TestIgnoredRepositoriesAreNotAllocatedAShare:
    def test_allocation_uses_repos_not_repo_dirs(self, two_repo_dirs, default_branch, cache_backend):
        """``repo_dirs`` is not filtered by ``ignore_repos`` on the Repository-instance path."""
        repos = [
            Repository(str(path), default_branch=default_branch, cache_backend=cache_backend, verbose=False)
            for path in two_repo_dirs
        ]
        projectd = ProjectDirectory(
            working_dir=repos,
            ignore_repos=[repos[1].repo_name],
            default_branch=default_branch,
            cache_backend=cache_backend,
            verbose=False,
        )

        # The divergence this test exists for: dividing by repo_dirs would over-divide by 2.
        assert len(projectd.repo_dirs) == 2
        assert len(projectd.repos) == 1

        assert projectd.commit_history(limit=5).shape[0] == 5
        assert projectd.file_change_history(limit=5).shape[0] == 5
        assert projectd.revs(limit=5).shape[0] == 5
        assert projectd.hours_estimate(limit=1)["committer"].tolist() == ["Dev 0"]


class TestEmptyProjectLimit:
    @pytest.fixture
    def empty_project(self, default_branch, cache_backend):
        projectd = ProjectDirectory(
            working_dir=[],
            default_branch=default_branch,
            cache_backend=cache_backend,
            verbose=False,
        )
        yield projectd
        projectd.__del__()

    def test_commit_history_returns_an_empty_frame(self, empty_project):
        df = empty_project.commit_history(limit=2)

        assert df.empty
        assert df.columns.tolist() == [
            "repository",
            "author",
            "committer",
            "date",
            "message",
            "commit_sha",
            "lines",
            "insertions",
            "deletions",
            "net",
        ]

    def test_file_change_history_returns_an_empty_frame(self, empty_project):
        df = empty_project.file_change_history(limit=2)

        assert df.empty
        assert df.columns.tolist() == [
            "repository",
            "date",
            "author",
            "committer",
            "message",
            "filename",
            "insertions",
            "deletions",
        ]

    def test_hours_estimate_returns_an_empty_frame(self, empty_project):
        df = empty_project.hours_estimate(limit=2)

        assert df.empty
        assert df.columns.tolist() == ["committer", "hours", "repository"]

    @pytest.mark.parametrize("kwargs", [{"limit": 2}, {"num_datapoints": 2}])
    def test_revs_returns_an_empty_frame(self, empty_project, kwargs):
        df = empty_project.revs(**kwargs)

        assert df.empty
        assert df.columns.tolist() == ["repository", "rev"]
