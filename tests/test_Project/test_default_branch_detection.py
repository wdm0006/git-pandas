import os
import subprocess

from gitpandas import ProjectDirectory


def _build_repo(path, branch):
    env = {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
    }
    subprocess.run(["git", "init", "-b", branch, path], check=True, env=env, capture_output=True)
    subprocess.run(["git", "-C", path, "config", "user.name", "Test User"], check=True, env=env)
    subprocess.run(["git", "-C", path, "config", "user.email", "test@example.com"], check=True, env=env)

    file_path = path / "tracked.txt"
    file_path.write_text(f"{branch}\n")
    subprocess.run(["git", "-C", path, "add", "tracked.txt"], check=True, env=env)
    subprocess.run(["git", "-C", path, "commit", "-m", f"{branch} commit"], check=True, env=env, capture_output=True)


def test_auto_detects_each_repository_default_branch(tmp_path):
    main_repo = tmp_path / "main-repo"
    master_repo = tmp_path / "master-repo"
    _build_repo(main_repo, "main")
    _build_repo(master_repo, "master")

    project = ProjectDirectory(working_dir=[str(main_repo), str(master_repo)])

    assert {repo.repo_name: repo.default_branch for repo in project.repos} == {
        "main-repo": "main",
        "master-repo": "master",
    }
    assert project.commit_history()["repository"].value_counts().to_dict() == {
        "main-repo": 1,
        "master-repo": 1,
    }
    assert project.file_change_rates()["repository"].value_counts().to_dict() == {
        "main-repo": 1,
        "master-repo": 1,
    }


def test_explicit_default_branch_still_applies_to_every_repository(tmp_path):
    main_repo = tmp_path / "main-repo"
    master_repo = tmp_path / "master-repo"
    _build_repo(main_repo, "main")
    _build_repo(master_repo, "master")

    project = ProjectDirectory(
        working_dir=[str(main_repo), str(master_repo)],
        default_branch="main",
    )

    assert [repo.default_branch for repo in project.repos] == ["main", "main"]
    assert project.commit_history()["repository"].value_counts().to_dict() == {"main-repo": 1}
