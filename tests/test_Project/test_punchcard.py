import warnings
from unittest.mock import Mock

import git
import numpy as np
import pandas as pd
import pytest
from git.exc import GitCommandError

from gitpandas import ProjectDirectory
from tests.test_Repository.test_punchcard import build_add_only_repo

METRICS = ["lines", "insertions", "deletions", "net"]


def assert_empty_punchcard(frame, by=None):
    columns = ["hour_of_day", "day_of_week"]
    if by is not None:
        columns.append(by)
    columns.extend(METRICS)

    assert frame.empty
    assert list(frame.columns) == columns


@pytest.mark.parametrize("by", [None, "committer"])
def test_punchcard_empty_project(by):
    project = ProjectDirectory(working_dir=[], verbose=False)

    assert_empty_punchcard(project.punchcard(by=by), by=by)


def test_punchcard_all_repositories_fail():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock(), Mock()]
    for repo in project.repos:
        repo.punchcard.side_effect = GitCommandError("punchcard failed", 128)

    assert_empty_punchcard(project.punchcard())


@pytest.mark.parametrize("by", [None, "committer"])
def test_punchcard_repository_without_commits(tmp_path, by):
    repo_path = tmp_path / "empty-repo"
    git.Repo.init(repo_path, initial_branch="master")
    project = ProjectDirectory(working_dir=[str(repo_path)], default_branch="master", verbose=False)

    assert_empty_punchcard(project.punchcard(by=by), by=by)


def test_punchcard_aggregates_values_without_future_warning():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock(repo_name="alpha"), Mock(repo_name="beta")]
    project.repos[0].punchcard.return_value = pd.DataFrame(
        {
            "hour_of_day": [9, 9],
            "day_of_week": [1, 1],
            "lines": [10, 5],
            "insertions": [8, 4],
            "deletions": [2, 1],
            "net": [6, 3],
        }
    )
    project.repos[1].punchcard.return_value = pd.DataFrame(
        {
            "hour_of_day": [9, 14],
            "day_of_week": [1, 3],
            "lines": [7, 20],
            "insertions": [5, 15],
            "deletions": [2, 5],
            "net": [3, 10],
        }
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        result = project.punchcard()

    assert result.to_dict("records") == [
        {"hour_of_day": 9, "day_of_week": 1, "lines": 22, "insertions": 17, "deletions": 5, "net": 12},
        {"hour_of_day": 14, "day_of_week": 3, "lines": 20, "insertions": 15, "deletions": 5, "net": 10},
    ]


def test_punchcard_normalizes_add_only_repository_to_finite_values(tmp_path, default_branch):
    add_only_repo = build_add_only_repo(tmp_path, default_branch)
    project = ProjectDirectory(working_dir=[], default_branch=default_branch, verbose=False)
    project.repos = [add_only_repo]

    punchcard = project.punchcard(normalize=100)

    assert np.isfinite(punchcard[METRICS].to_numpy()).all()
    assert punchcard["deletions"].tolist() == [0.0] * 6
    expected = [200 / 11, 200 / 11, 200 / 11, 100 / 11, 200 / 11, 200 / 11]
    for metric in ["lines", "insertions", "net"]:
        assert sorted(punchcard[metric]) == pytest.approx(sorted(expected))
        assert punchcard[metric].sum() == pytest.approx(100)
