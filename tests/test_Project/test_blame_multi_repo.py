import git
import pytest

from gitpandas import ProjectDirectory


@pytest.fixture
def single_author_project(tmp_path, default_branch):
    repos_dir = tmp_path / "repos"
    repos_dir.mkdir()

    for repo_number in range(3):
        repo_dir = repos_dir / f"repo{repo_number}"
        repo_dir.mkdir()
        repo = git.Repo.init(repo_dir)
        repo.git.config("user.name", "Alice")
        repo.git.config("user.email", "alice@example.com")
        file_name = f"file{repo_number}.py"
        (repo_dir / file_name).write_text("first\nsecond\nthird\n")
        repo.git.add(file_name)
        repo.git.commit(m=f"add {file_name}")

    return ProjectDirectory(working_dir=str(repos_dir), default_branch=default_branch)


def test_blame_aggregates_committer_across_repositories(single_author_project):
    blame = single_author_project.blame()

    assert blame.index.tolist() == ["Alice"]
    assert blame.index.name == "committer"
    assert blame.loc["Alice", "loc"] == 9


def test_blame_by_file_preserves_committer_and_file(single_author_project):
    blame = single_author_project.blame(by="file")

    assert blame.index.names == ["committer", "file"]
    assert blame["loc"].to_dict() == {
        ("Alice", "file0.py"): 3,
        ("Alice", "file1.py"): 3,
        ("Alice", "file2.py"): 3,
    }


def test_project_bus_factor_uses_aggregated_committers(single_author_project):
    bus_factor = single_author_project.bus_factor(by="projectd")

    assert bus_factor.loc[0, "bus factor"] == 1


def test_project_blame_loc_matches_repository_totals(single_author_project):
    project_total = single_author_project.blame()["loc"].sum()
    repository_total = sum(repo.blame()["loc"].sum() for repo in single_author_project.repos)

    assert project_total == repository_total == 9
