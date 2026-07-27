import os
import subprocess

import pandas as pd
import pytest

from gitpandas import Repository


@pytest.fixture
def rev_to_rev_repo(tmp_path):
    repo_path = tmp_path / "rev-to-rev-repo"
    git_env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
    }
    subprocess.run(["git", "init", "-b", "master", repo_path], check=True, env=git_env, capture_output=True)

    def commit(message, timestamp, author, committer):
        subprocess.run(["git", "-C", repo_path, "add", "."], check=True, env=git_env, capture_output=True)
        commit_env = {
            **git_env,
            "GIT_AUTHOR_NAME": author,
            "GIT_AUTHOR_EMAIL": f"{author.lower().replace(' ', '.')}@example.com",
            "GIT_AUTHOR_DATE": timestamp,
            "GIT_COMMITTER_NAME": committer,
            "GIT_COMMITTER_EMAIL": f"{committer.lower().replace(' ', '.')}@example.com",
            "GIT_COMMITTER_DATE": timestamp,
        }
        subprocess.run(
            ["git", "-C", repo_path, "commit", "-m", message],
            check=True,
            env=commit_env,
            capture_output=True,
        )

    (repo_path / "app.py").write_text("keep\nold python\nremove python\n")
    (repo_path / "README.md").write_text("keep\nold markdown\nremove markdown\n")
    (repo_path / "web.js").write_text("keep\nold javascript\n")
    commit("initial files", "2024-01-01T12:00:00+00:00", "Initial Author", "Initial Committer")
    subprocess.run(["git", "-C", repo_path, "tag", "v1.0.0"], check=True, env=git_env, capture_output=True)

    (repo_path / "README.md").write_text("keep\nnew markdown\n")
    commit("update documentation", "2024-01-02T12:00:00+00:00", "Alice Author", "Carol Committer")

    (repo_path / "app.py").write_text("keep\nnew python one\nnew python two\nnew python three\n")
    (repo_path / "web.js").write_text("keep\nnew javascript one\nnew javascript two\n")
    commit("update code", "2024-01-03T12:00:00+00:00", "Bob Author", "Dave Committer")
    subprocess.run(["git", "-C", repo_path, "tag", "v2.0.0"], check=True, env=git_env, capture_output=True)

    return Repository(working_dir=str(repo_path), default_branch="master")


def test_time_between_revs_has_exact_day_delta(rev_to_rev_repo):
    assert rev_to_rev_repo.time_between_revs("v1.0.0", "v2.0.0") == 2.0


def test_diff_stats_between_revs_has_exact_values(rev_to_rev_repo):
    stats = rev_to_rev_repo.diff_stats_between_revs("v1.0.0", "v2.0.0")
    stats["files"] = sorted(stats["files"])

    assert stats == {
        "insertions": 6,
        "deletions": 5,
        "net": 1,
        "files_changed": 3,
        "files": ["README.md", "app.py", "web.js"],
    }


@pytest.mark.parametrize(
    ("globs", "expected"),
    [
        ({"include_globs": ["*.py"]}, (3, 2, 1, ["app.py"])),
        ({"ignore_globs": ["*.md"]}, (5, 3, 2, ["app.py", "web.js"])),
    ],
)
def test_diff_stats_between_revs_filters_globs(rev_to_rev_repo, globs, expected):
    stats = rev_to_rev_repo.diff_stats_between_revs("v1.0.0", "v2.0.0", **globs)

    assert (stats["insertions"], stats["deletions"], stats["files_changed"], sorted(stats["files"])) == expected
    assert stats["net"] == stats["insertions"] - stats["deletions"]


def test_committers_between_revs_has_exact_names(rev_to_rev_repo):
    assert rev_to_rev_repo.committers_between_revs("v1.0.0", "v2.0.0") == {
        "committers": ["Carol Committer", "Dave Committer"],
        "authors": ["Alice Author", "Bob Author"],
    }


@pytest.mark.parametrize("globs", [{"include_globs": ["*.py"]}, {"ignore_globs": ["*.md"]}])
def test_committers_between_revs_filters_globs(rev_to_rev_repo, globs):
    assert rev_to_rev_repo.committers_between_revs("v1.0.0", "v2.0.0", **globs) == {
        "committers": ["Dave Committer"],
        "authors": ["Bob Author"],
    }


@pytest.mark.parametrize(
    ("globs", "expected"),
    [
        ({}, ["README.md", "app.py", "web.js"]),
        ({"include_globs": ["*.py"]}, ["app.py"]),
        ({"ignore_globs": ["*.md"]}, ["app.py", "web.js"]),
    ],
)
def test_files_changed_between_revs_has_exact_files(rev_to_rev_repo, globs, expected):
    assert rev_to_rev_repo.files_changed_between_revs("v1.0.0", "v2.0.0", **globs) == expected


def test_release_tag_summary_has_exact_values(rev_to_rev_repo):
    summary = rev_to_rev_repo.release_tag_summary(tag_glob="v*")

    assert summary["tag"].tolist() == ["v1.0.0", "v2.0.0"]
    assert pd.isna(summary.iloc[0]["time_since_prev"])
    assert summary.iloc[0][["insertions", "deletions", "net", "files_changed"]].tolist() == [0, 0, 0, 0]
    assert summary.iloc[1]["time_since_prev"] == 2.0
    assert summary.iloc[1][["insertions", "deletions", "net", "files_changed"]].tolist() == [6, 5, 1, 3]
    assert summary.iloc[1]["committers"] == ["Carol Committer", "Dave Committer"]
    assert summary.iloc[1]["authors"] == ["Alice Author", "Bob Author"]
    assert sorted(summary.iloc[1]["files"]) == ["README.md", "app.py", "web.js"]


@pytest.mark.parametrize("globs", [{"include_globs": ["*.py"]}, {"ignore_globs": ["*.md"]}])
def test_release_tag_summary_matches_direct_glob_results(rev_to_rev_repo, globs):
    summary = rev_to_rev_repo.release_tag_summary(tag_glob="v*", **globs).iloc[1]
    stats = rev_to_rev_repo.diff_stats_between_revs("v1.0.0", "v2.0.0", **globs)
    contributors = rev_to_rev_repo.committers_between_revs("v1.0.0", "v2.0.0", **globs)
    files = rev_to_rev_repo.files_changed_between_revs("v1.0.0", "v2.0.0", **globs)

    assert summary[["insertions", "deletions", "net", "files_changed"]].to_dict() == {
        key: stats[key] for key in ("insertions", "deletions", "net", "files_changed")
    }
    assert summary["committers"] == contributors["committers"]
    assert summary["authors"] == contributors["authors"]
    assert summary["files"] == files
