"""Regression tests for file_owner honouring the ``committer`` flag.

The blame extraction in ``Repository.file_owner`` used to always read
``x[0].committer.name``, so ``committer=False`` returned the top committer
under a column labelled ``author``.
"""

import git
import pytest

from gitpandas import Repository
from gitpandas.cache import EphemeralCache

AUTHORS = ["Alice Author", "Carol Author"]
COMMITTERS = ["Bob Committer", "Dave Committer"]


def _commit(repo, message, author, committer):
    """Commit the staged tree with a distinct author and committer."""
    repo.git.update_environment(
        GIT_COMMITTER_NAME=committer,
        GIT_COMMITTER_EMAIL=f"{committer.split()[0].lower()}@example.com",
    )
    repo.git.commit(m=message, author=f"{author} <{author.split()[0].lower()}@example.com>")


def _build_repo(repo_dir, default_branch, filename="owned.py"):
    """Build a repo whose top author and top committer are different people."""
    repo_dir.mkdir(parents=True)
    repo = git.Repo.init(str(repo_dir))
    repo.git.config("user.name", "Fallback User")
    repo.git.config("user.email", "fallback@example.com")
    repo.git.checkout("-b", default_branch)

    target = repo_dir / filename
    target.write_text("one\ntwo\nthree\nfour\nfive\n")
    repo.git.add(all=True)
    _commit(repo, "majority lines", AUTHORS[0], COMMITTERS[0])

    target.write_text("one\ntwo\nthree\nfour\nfive\nsix\n")
    repo.git.add(all=True)
    _commit(repo, "one more line", AUTHORS[1], COMMITTERS[1])

    return repo_dir


@pytest.fixture
def repo_dir(tmp_path, default_branch):
    return _build_repo(tmp_path / "authored_repo", default_branch)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def repository(request, repo_dir, default_branch):
    cache_backend = EphemeralCache() if request.param else None
    repo = Repository(working_dir=str(repo_dir), default_branch=default_branch, cache_backend=cache_backend)
    yield repo
    repo.__del__()


class TestFileOwnerIdentity:
    def test_file_owner_respects_committer_flag(self, repository):
        assert repository.file_owner("HEAD", "owned.py", committer=True) == {"name": "Bob Committer"}
        assert repository.file_owner("HEAD", "owned.py", committer=False) == {"name": "Alice Author"}

    def test_file_owner_agrees_with_blame(self, repository):
        for committer in (True, False):
            column = "committer" if committer else "author"
            blame = repository.blame(rev="HEAD", committer=committer)
            top = blame["loc"].idxmax()

            assert blame.index.name == column
            assert repository.file_owner("HEAD", "owned.py", committer=committer) == {"name": top}

    def test_file_detail_owner_matches_flag(self, repository):
        committer_detail = repository.file_detail(committer=True)
        author_detail = repository.file_detail(committer=False)

        assert committer_detail["file_owner"].tolist() == ["Bob Committer"]
        assert author_detail["file_owner"].tolist() == ["Alice Author"]
