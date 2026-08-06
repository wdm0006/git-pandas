import datetime

import pytest
from git import Actor, Repo

from gitpandas import Repository


def _build_linear_repo(path, shared_timestamp):
    git_repo = Repo.init(path)
    git_repo.git.checkout(b="main")
    actor = Actor("Test Author", "test@example.com")
    base_timestamp = int(datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc).timestamp())
    test_file = path / "history.txt"

    for line_count in range(1, 6):
        timestamp = base_timestamp if shared_timestamp else base_timestamp + (line_count * 3600)
        test_file.write_text("".join(f"line {line}\n" for line in range(1, line_count + 1)))
        git_repo.index.add(["history.txt"])
        git_repo.index.commit(
            f"Add line {line_count}",
            author=actor,
            committer=actor,
            author_date=f"{timestamp} +0000",
            commit_date=f"{timestamp} +0000",
        )


@pytest.mark.parametrize("method_name", ["cumulative_blame", "parallel_cumulative_blame"])
@pytest.mark.parametrize("shared_timestamp", [True, False], ids=["duplicate-dates", "distinct-dates"])
def test_cumulative_blame_preserves_each_revision(tmp_path, method_name, shared_timestamp):
    repo_path = tmp_path / "linear_repo"
    repo_path.mkdir()
    _build_linear_repo(repo_path, shared_timestamp)
    repo = Repository(working_dir=str(repo_path), default_branch="main")

    revs = repo.revs(branch="main")
    result = getattr(repo, method_name)(branch="main", workers=2) if method_name.startswith("parallel") else getattr(
        repo, method_name
    )(branch="main")

    assert len(result) == len(revs) == 5
    assert result.index.is_monotonic_increasing

    if shared_timestamp:
        # every revision shares one timestamp, so sorting is a stable no-op and the
        # revisions stay in the newest-first order revs() produced them in
        assert result["Test Author"].tolist() == [5, 4, 3, 2, 1]
    else:
        assert result["Test Author"].tolist() == [1, 2, 3, 4, 5]
