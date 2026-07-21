from unittest.mock import Mock

import pandas as pd
from git import GitCommandError

from gitpandas import ProjectDirectory


def commits_in_tags_frame(repository, tag, commit_sha, tag_date, commit_date):
    return pd.DataFrame(
        {
            "commit_sha": [commit_sha],
            "tag": [tag],
            "repository": [repository],
            "tag_date": [pd.Timestamp(tag_date, tz="UTC")],
            "commit_date": [pd.Timestamp(commit_date, tz="UTC")],
        }
    ).set_index(["tag_date", "commit_date"])


def assert_empty_commits_in_tags(df):
    assert df.empty
    assert list(df.columns) == ["commit_sha", "tag", "repository"]
    assert list(df.index.names) == ["tag_date", "commit_date"]


def test_commits_in_tags_empty_project():
    project = ProjectDirectory(working_dir=[], verbose=False)

    assert_empty_commits_in_tags(project.commits_in_tags())


def test_commits_in_tags_all_repositories_fail():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock(), Mock()]
    for repo in project.repos:
        repo.commits_in_tags.side_effect = GitCommandError("commits_in_tags failed", 128)

    assert_empty_commits_in_tags(project.commits_in_tags())


def test_commits_in_tags_concatenates_repository_results():
    project = ProjectDirectory(working_dir=[], verbose=False)
    project.repos = [Mock(), Mock()]
    project.repos[0].commits_in_tags.return_value = commits_in_tags_frame(
        "alpha", "v1.0", "aaa111", "2024-01-02", "2024-01-01"
    )
    project.repos[1].commits_in_tags.return_value = commits_in_tags_frame(
        "beta", "v2.0", "bbb222", "2024-02-02", "2024-02-01"
    )

    result = project.commits_in_tags(start="2024-01-01")

    assert result.reset_index().to_dict("records") == [
        {
            "tag_date": pd.Timestamp("2024-01-02", tz="UTC"),
            "commit_date": pd.Timestamp("2024-01-01", tz="UTC"),
            "commit_sha": "aaa111",
            "tag": "v1.0",
            "repository": "alpha",
        },
        {
            "tag_date": pd.Timestamp("2024-02-02", tz="UTC"),
            "commit_date": pd.Timestamp("2024-02-01", tz="UTC"),
            "commit_sha": "bbb222",
            "tag": "v2.0",
            "repository": "beta",
        },
    ]
    for repo in project.repos:
        repo.commits_in_tags.assert_called_once_with(start="2024-01-01")
