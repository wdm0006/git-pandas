import os
import subprocess

import pytest

from gitpandas import Repository


@pytest.fixture
def hours_repo(tmp_path):
    repo_path = tmp_path / "hours-repo"
    git_env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
    }
    subprocess.run(["git", "init", "-b", "master", repo_path], check=True, env=git_env, capture_output=True)

    commits = [
        ("Alice", "alice@example.com", "2024-01-01T09:00:00+00:00"),
        ("Alice", "alice@example.com", "2024-01-01T09:20:00+00:00"),
        ("Alice", "alice@example.com", "2024-01-01T09:40:00+00:00"),
        ("Alice", "alice@example.com", "2024-01-05T09:40:00+00:00"),
        ("Bob", "bob@example.com", "2024-01-06T09:00:00+00:00"),
    ]
    for number, (name, email, timestamp) in enumerate(commits):
        (repo_path / "work.txt").write_text(f"commit {number}\n")
        subprocess.run(["git", "-C", repo_path, "add", "work.txt"], check=True, env=git_env, capture_output=True)
        commit_env = {
            **git_env,
            "GIT_AUTHOR_NAME": name,
            "GIT_AUTHOR_EMAIL": email,
            "GIT_AUTHOR_DATE": timestamp,
            "GIT_COMMITTER_NAME": name,
            "GIT_COMMITTER_EMAIL": email,
            "GIT_COMMITTER_DATE": timestamp,
        }
        subprocess.run(
            ["git", "-C", repo_path, "commit", "-m", f"commit {number}"],
            check=True,
            env=commit_env,
            capture_output=True,
        )

    return Repository(working_dir=str(repo_path), default_branch="master")


def _hours_by_committer(repo, **kwargs):
    result = repo.hours_estimate(**kwargs)
    return result.set_index("committer")["hours"].to_dict()


def test_single_commit_contributor_gets_single_commit_allowance(hours_repo):
    hours = _hours_by_committer(hours_repo)

    assert hours["Bob"] == pytest.approx(0.5)


def test_hours_estimate_counts_first_commit_and_new_session(hours_repo):
    hours = _hours_by_committer(hours_repo)

    assert hours["Alice"] == pytest.approx(0.5 + 1 / 3 + 1 / 3 + 0.5)


def test_hours_estimate_scales_single_commit_allowance(hours_repo):
    default_hours = _hours_by_committer(hours_repo)
    increased_hours = _hours_by_committer(hours_repo, single_commit_hours=1.0)

    assert increased_hours["Alice"] - default_hours["Alice"] == pytest.approx(1.0)
    assert increased_hours["Bob"] - default_hours["Bob"] == pytest.approx(0.5)
