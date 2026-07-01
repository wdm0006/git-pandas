import git
import pytest

from gitpandas import ProjectDirectory


@pytest.fixture
def skewed_repo_dir(tmp_path, default_branch):
    """A single repo where one author dominates via one large, ignorable file.

    Layout of line ownership:
        - "Bot" owns ``generated.py`` (300 lines) -> dominates the whole repo
        - Three humans each own a tiny file (10 lines) with equal weight

    With all files counted, ``generated.py`` alone exceeds 50% of the LOC, so
    the bus factor is 1. Ignoring ``generated.py`` leaves three equal authors,
    pushing the bus factor to 2. This makes ``ignore_globs`` verifiably change
    the result.
    """
    repos_dir = tmp_path / "repos"
    repos_dir.mkdir()
    repo_dir = repos_dir / "repo1"
    repo_dir.mkdir()

    grepo = git.Repo.init(str(repo_dir))

    def commit_file(name, num_lines, author, email):
        grepo.git.config("user.name", author)
        grepo.git.config("user.email", email)
        (repo_dir / name).write_text("".join(f"x_{i} = {i}\n" for i in range(num_lines)))
        grepo.git.add(name)
        grepo.git.commit(m=f"add {name}")

    commit_file("generated.py", 300, "Bot", "bot@example.com")
    commit_file("a.py", 10, "User A", "usera@example.com")
    commit_file("b.py", 10, "User B", "userb@example.com")
    commit_file("c.py", 10, "User C", "userc@example.com")

    yield repos_dir


class TestProjectBusFactorIgnoreGlobs:
    def test_ignore_globs_respected_by_repository(self, skewed_repo_dir, default_branch):
        """Regression: bus_factor(by='repository') must honor ignore_globs.

        Previously the repository branch passed ``include_globs`` into the
        per-repo ``ignore_globs`` argument, silently dropping the caller's
        ``ignore_globs``. This test fails before that fix and passes after.
        """
        pd_obj = ProjectDirectory(working_dir=str(skewed_repo_dir), default_branch=default_branch)

        bf_all = pd_obj.bus_factor(by="repository")
        bf_ignored = pd_obj.bus_factor(by="repository", ignore_globs=["generated.py"])

        # The dominant generated file makes the bus factor 1 when everything counts.
        assert bf_all["bus factor"].values[0] == 1

        # Ignoring it leaves three equal contributors, raising the bus factor to 2.
        assert bf_ignored["bus factor"].values[0] == 2

        # The ignored result must differ from the unfiltered one.
        assert bf_ignored["bus factor"].values[0] > bf_all["bus factor"].values[0]
