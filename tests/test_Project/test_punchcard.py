from unittest.mock import Mock

import pandas as pd
from git import GitCommandError

from gitpandas import ProjectDirectory


def punchcard_chunk(repository, hour, day, lines, insertions, deletions, net):
    return pd.DataFrame(
        {
            "hour_of_day": [hour],
            "day_of_week": [day],
            "lines": [lines],
            "insertions": [insertions],
            "deletions": [deletions],
            "net": [net],
        }
    )


def assert_empty_punchcard(df, by=None):
    assert df.empty
    expected_cols = ["hour_of_day", "day_of_week"]
    if by is not None:
        expected_cols.append(by)
    expected_cols += ["lines", "insertions", "deletions", "net"]
    assert list(df.columns) == expected_cols


def test_punchcard_empty_project():
    project = ProjectDirectory(working_dir=[], verbose=False)

    assert_empty_punchcard(project.punchcard())


def test_punchcard_all_repositories_fail():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock(), Mock()]
    for repo in project.repos:
        repo.punchcard.side_effect = GitCommandError("punchcard failed", 128)

    assert_empty_punchcard(project.punchcard())


def test_punchcard_no_commits_on_branch():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock()]
    project.repos[0].repo_name = "alpha"
    project.repos[0].punchcard.return_value = pd.DataFrame(
        columns=["hour_of_day", "day_of_week", "lines", "insertions", "deletions", "net"]
    )

    assert_empty_punchcard(project.punchcard())


def test_punchcard_concatenates_repository_results():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock(), Mock()]
    project.repos[0].repo_name = "alpha"
    project.repos[0].punchcard.return_value = punchcard_chunk("alpha", 9, 0, 10, 8, 2, 6)
    project.repos[1].repo_name = "beta"
    project.repos[1].punchcard.return_value = punchcard_chunk("beta", 9, 0, 5, 5, 0, 5)

    result = project.punchcard()

    row = result[(result["hour_of_day"] == 9) & (result["day_of_week"] == 0)].iloc[0]
    assert row["lines"] == 15
    assert row["insertions"] == 13
    assert row["deletions"] == 2
    assert row["net"] == 11
