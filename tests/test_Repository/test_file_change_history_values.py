import datetime

import pytest
from git import Repo

from gitpandas import Repository


@pytest.fixture
def change_history_repo(tmp_path):
    repo_path = tmp_path / "change_history"
    repo = Repo.init(repo_path, initial_branch="master")
    repo.config_writer().set_value("user", "name", "Test User").release()
    repo.config_writer().set_value("user", "email", "test@example.com").release()

    contents = [
        ("root", "a\nb\nc\n"),
        ("add", "a\nb\nc\nd\ne\n"),
        ("delete", "a\nb\nd\ne\n"),
        ("mixed", "a\nB\nd\ne\nf\n"),
    ]
    start = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
    for offset, (message, content) in enumerate(contents):
        (repo_path / "f.txt").write_text(content)
        repo.index.add(["f.txt"])
        commit_date = int((start + datetime.timedelta(hours=offset)).timestamp())
        repo.index.commit(message, commit_date=f"{commit_date} +0000")

    return Repository(working_dir=str(repo_path), default_branch="master")


def test_file_change_history_reports_exact_churn(change_history_repo):
    history = change_history_repo.file_change_history(branch="master")
    actual = {
        row.message: (row.insertions, row.deletions, row.lines)
        for row in history.itertuples()
    }

    assert actual == {
        "root": (3, 0, 3),
        "add": (2, 0, 2),
        "delete": (0, 1, -1),
        "mixed": (2, 1, 1),
    }


def test_file_change_history_totals_match_commit_history(change_history_repo):
    file_history = change_history_repo.file_change_history(branch="master")
    file_totals = file_history.groupby("message")[["insertions", "deletions"]].sum()
    commit_history = change_history_repo.commit_history(branch="master").set_index("message")

    for message in ["root", "add", "delete", "mixed"]:
        assert tuple(file_totals.loc[message]) == (
            commit_history.loc[message, "insertions"],
            commit_history.loc[message, "deletions"],
        )


def test_file_change_rates_reports_nonzero_churn(change_history_repo):
    rates = change_history_repo.file_change_rates(branch="master").set_index("file")

    assert rates.loc["f.txt", "abs_change"] == 9
    assert rates.loc["f.txt", "net_change"] == 5
