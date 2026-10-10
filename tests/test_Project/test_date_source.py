"""``ProjectDirectory`` forwards ``date_source`` to every member repository."""

import pytest

from gitpandas import ProjectDirectory
from gitpandas.cache import EphemeralCache
from tests.test_Repository.test_date_source import (
    COLLAPSED,
    EXPECTED_A,
    EXPECTED_B,
    LAYOUT_A,
    LAYOUT_B,
    REBASE_EPOCH,
    build_rebased_repo,
)


@pytest.fixture(scope="module")
def project_dirs(tmp_path_factory, default_branch):
    root = tmp_path_factory.mktemp("date_source_project")
    return [
        str(build_rebased_repo(root / "repo_a", default_branch, LAYOUT_A)),
        str(build_rebased_repo(root / "repo_b", default_branch, LAYOUT_B)),
    ]


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def project(request, project_dirs, default_branch):
    cache = EphemeralCache() if request.param else None
    return ProjectDirectory(working_dir=project_dirs, cache_backend=cache, default_branch=default_branch, verbose=False)


def test_project_hours_author_date(project):
    df = project.hours_estimate(committer=False, date_source="author")
    assert dict(zip(df["author"], df["hours"], strict=True)) == pytest.approx({**EXPECTED_A, **EXPECTED_B})


def test_project_hours_committer_date_collapsed(project):
    df = project.hours_estimate(committer=False, date_source="committer")
    names = {n for n, _ in LAYOUT_A + LAYOUT_B}
    assert dict(zip(df["author"], df["hours"], strict=True)) == pytest.approx(dict.fromkeys(names, COLLAPSED))


def test_project_commit_history_dates(project):
    committer = project.commit_history(date_source="committer")
    author = project.commit_history(date_source="author")
    assert {ts.timestamp() for ts in committer["date"]} == {REBASE_EPOCH}
    # LAYOUT_A and LAYOUT_B share minute offsets 0 and 600, so 8 distinct author timestamps remain.
    assert author["date"].nunique() == len({m for _, m in LAYOUT_A} | {m for _, m in LAYOUT_B})


def test_project_forwards_to_each_repo(project):
    seen = []
    for repo in project.repos:
        original = repo.commit_history

        def spy(*args, _orig=original, **kwargs):
            seen.append(kwargs.get("date_source"))
            return _orig(*args, **kwargs)

        repo.commit_history = spy
    project.commit_history(date_source="author")
    assert seen == ["author", "author"]


@pytest.mark.parametrize("method", ["commit_history", "hours_estimate"])
def test_project_invalid_date_source(project, method):
    with pytest.raises(ValueError, match="date_source"):
        getattr(project, method)(date_source="bogus")


def test_project_invalid_date_source_empty_project(tmp_path):
    empty = ProjectDirectory(working_dir=[], verbose=False)
    with pytest.raises(ValueError, match="date_source"):
        empty.hours_estimate(date_source="bogus")
