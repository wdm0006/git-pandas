import os
import subprocess
import warnings

import pandas as pd
import pytest

from gitpandas import Repository
from gitpandas.cache import EphemeralCache

# Fixed epoch so tag/commit timestamps -- and therefore the start/end bounds -- are reproducible.
BASE_EPOCH = 1600000000
COMMIT_COUNT = 6
TAGGED_COMMITS = {3: "v1.0", 6: "v2.0"}

GIT_ENV = {
    **os.environ,
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
}


def _epoch(commit_number):
    return BASE_EPOCH + commit_number * 3600


def _timestamp(epoch):
    return pd.Timestamp(epoch, unit="s", tz="UTC")


def _git(repo_path, *args, env=None):
    subprocess.run(
        ["git", "-C", str(repo_path), *args],
        check=True,
        capture_output=True,
        env=env or GIT_ENV,
    )


@pytest.fixture
def tagged_repo(tmp_path):
    """Six commits with annotated tags on commits 3 and 6, at pinned timestamps."""
    repo_path = tmp_path / "tagged-repo"
    subprocess.run(
        ["git", "init", "-b", "master", str(repo_path)],
        check=True,
        capture_output=True,
        env=GIT_ENV,
    )
    _git(repo_path, "config", "user.name", "Tester")
    _git(repo_path, "config", "user.email", "tester@example.com")

    shas = {}
    for number in range(1, COMMIT_COUNT + 1):
        (repo_path / "work.txt").write_text(f"commit {number}\n")
        _git(repo_path, "add", "work.txt")
        stamped = {
            **GIT_ENV,
            "GIT_AUTHOR_DATE": f"{_epoch(number)} +0000",
            "GIT_COMMITTER_DATE": f"{_epoch(number)} +0000",
        }
        _git(repo_path, "commit", "-m", f"commit {number}", env=stamped)
        shas[number] = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            env=GIT_ENV,
        ).stdout.strip()
        if number in TAGGED_COMMITS:
            tag_name = TAGGED_COMMITS[number]
            _git(repo_path, "tag", "-a", tag_name, "-m", tag_name, env=stamped)

    return repo_path, shas


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def repo(request, tagged_repo):
    repo_path, shas = tagged_repo
    cache_backend = EphemeralCache() if request.param else None
    return Repository(working_dir=str(repo_path), default_branch="master", cache_backend=cache_backend), shas


def _attribution(result):
    return dict(zip(result["commit_sha"], result["tag"], strict=True))


def test_every_commit_is_attributed_to_the_release_that_shipped_it(repo):
    repository, shas = repo

    result = repository.commits_in_tags(start=_timestamp(BASE_EPOCH))

    expected = {shas[number]: ("v1.0" if number <= 3 else "v2.0") for number in shas}
    assert _attribution(result) == expected


def test_row_count_matches_commit_count_with_no_duplicates(repo):
    repository, _ = repo

    result = repository.commits_in_tags(start=_timestamp(BASE_EPOCH))

    assert len(result) == COMMIT_COUNT
    assert result["commit_sha"].is_unique


def test_start_bound_prunes_commits_from_earlier_releases(repo):
    repository, shas = repo

    # Half way between the v1.0 commit and the first commit of the v2.0 release.
    result = repository.commits_in_tags(start=_timestamp(_epoch(3) + 1800))

    assert _attribution(result) == {shas[number]: "v2.0" for number in (4, 5, 6)}


def test_end_bound_prunes_commits_from_later_releases(repo):
    repository, shas = repo

    result = repository.commits_in_tags(start=_timestamp(BASE_EPOCH), end=_timestamp(_epoch(3) + 1800))

    assert _attribution(result) == {shas[number]: "v1.0" for number in (1, 2, 3)}


def test_commits_in_tags_emits_no_future_warning(repo):
    repository, _ = repo

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        repository.commits_in_tags(start=_timestamp(BASE_EPOCH))


def test_walks_history_longer_than_the_python_recursion_limit(tmp_path):
    """A tag whose release spans thousands of commits must not exhaust the interpreter stack."""
    commit_count = 1200
    repo_path = tmp_path / "deep-repo"
    subprocess.run(
        ["git", "init", "-b", "master", str(repo_path)],
        check=True,
        capture_output=True,
        env=GIT_ENV,
    )

    stream = []
    for number in range(1, commit_count + 1):
        message = f"commit {number}"
        body = str(number)
        stream += [
            "commit refs/heads/master",
            f"mark :{number}",
            f"committer Tester <tester@example.com> {_epoch(number)} +0000",
            f"data {len(message)}",
            message,
        ]
        if number > 1:
            stream.append(f"from :{number - 1}")
        stream += ["M 644 inline work.txt", f"data {len(body)}", body]
    stream += [
        "tag v1.0",
        f"from :{commit_count}",
        f"tagger Tester <tester@example.com> {_epoch(commit_count)} +0000",
        "data 4",
        "v1.0",
    ]
    subprocess.run(
        ["git", "-C", str(repo_path), "fast-import", "--quiet"],
        input="\n".join(stream).encode(),
        check=True,
        capture_output=True,
        env=GIT_ENV,
    )
    _git(repo_path, "reset", "--hard", "master")

    repository = Repository(working_dir=str(repo_path), default_branch="master")
    result = repository.commits_in_tags(start=_timestamp(BASE_EPOCH))

    assert len(result) == commit_count
    assert set(result["tag"]) == {"v1.0"}
