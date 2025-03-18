import git
import pytest

from gitpandas import ProjectDirectory

__author__ = "willmcginnis"


@pytest.fixture
def remote_project():
    """Fixture for a remote project directory."""
    project = ProjectDirectory(working_dir=["https://github.com/wdm0006/git-pandas.git"], verbose=True)
    yield project
    project.__del__()


@pytest.fixture
def local_project(tmp_path):
    """Fixture for a local project directory with multiple repositories."""
    # Create a temporary directory for the project
    project_dir = tmp_path / "repos"
    project_dir.mkdir()

    # Create repository directories
    repo1_dir = project_dir / "repository1"
    repo2_dir = project_dir / "repository2"
    repo1_dir.mkdir()
    repo2_dir.mkdir()

    # Initialize git repos
    grepo1 = git.Repo.init(str(repo1_dir))
    grepo2 = git.Repo.init(str(repo2_dir))

    # Configure git user
    grepo1.git.config("user.name", "Test User")
    grepo1.git.config("user.email", "test@example.com")
    grepo2.git.config("user.name", "Test User")
    grepo2.git.config("user.email", "test@example.com")

    # Rename master to main
    grepo1.git.branch("-M", "main")
    grepo2.git.branch("-M", "main")

    # Add README files
    with open(f"{repo1_dir}/README.md", "w") as f:
        f.write("Sample README for a sample python project\n")

    with open(f"{repo2_dir}/README.md", "w") as f:
        f.write("Sample README for a sample js project\n")

    # Commit them
    grepo1.git.add("README.md")
    grepo1.git.commit(m="first commit")

    grepo2.git.add("README.md")
    grepo2.git.commit(m="first commit")

    # Add Python files to repo1
    for idx in range(5):  # Increased from 3 to 5 files
        with open(f"{repo1_dir}/file_{idx}.py", "w") as f:
            f.write("import sys\nimport os\n")

        grepo1.git.add(all=True)
        grepo1.git.commit(m=f"adding file_{idx}.py")

    # Add JS files to repo2
    for idx in range(5):  # Increased from 3 to 5 files
        with open(f"{repo2_dir}/file_{idx}.js", "w") as f:
            f.write('document.write("hello world!");\n')

        grepo2.git.add(all=True)
        grepo2.git.commit(m=f"adding file_{idx}.js")

    # Create ProjectDirectory objects
    projectd_1 = ProjectDirectory(working_dir=[str(repo1_dir), str(repo2_dir)], verbose=True)
    projectd_2 = ProjectDirectory(working_dir=str(project_dir), verbose=True)

    yield {"projectd_1": projectd_1, "projectd_2": projectd_2}

    # Cleanup
    projectd_1.__del__()
    projectd_2.__del__()


# Remote project tests
class TestRemoteProperties:
    @pytest.mark.remote
    def test_repo_name(self, remote_project):
        assert "git-pandas" in list(remote_project.repo_name()["repository"].values)

    @pytest.mark.remote
    def test_branches(self, remote_project):
        branches = list(remote_project.branches()["branch"].values)
        assert "master" in branches
        assert "gh-pages" in branches

    @pytest.mark.remote
    def test_tags(self, remote_project):
        tags = list(remote_project.tags()["tag"].values)
        assert "0.0.1" in tags
        assert "0.0.2" in tags

    @pytest.mark.remote
    def test_is_bare(self, remote_project):
        for x in remote_project.is_bare()["is_bare"].values:
            assert not x


# Local project tests
class TestLocalProperties:
    def test_repo_name(self, local_project):
        projectd_1 = local_project["projectd_1"]
        projectd_2 = local_project["projectd_2"]

        assert "repository1" in list(projectd_1.repo_name()["repository"].values)
        assert "repository2" in list(projectd_1.repo_name()["repository"].values)
        assert "repository1" in list(projectd_2.repo_name()["repository"].values)
        assert "repository2" in list(projectd_2.repo_name()["repository"].values)

    def test_branches(self, local_project):
        projectd_1 = local_project["projectd_1"]
        projectd_2 = local_project["projectd_2"]

        branches = list(projectd_1.branches()["branch"].values)
        assert "main" in branches

        branches = list(projectd_2.branches()["branch"].values)
        assert "main" in branches

    def test_tags(self, local_project):
        projectd_1 = local_project["projectd_1"]
        projectd_2 = local_project["projectd_2"]

        tags = projectd_1.tags()
        assert len(tags) == 0

        tags = projectd_2.tags()
        assert len(tags) == 0

    def test_is_bare(self, local_project):
        projectd_1 = local_project["projectd_1"]
        projectd_2 = local_project["projectd_2"]

        bares = projectd_1.is_bare()["is_bare"].values
        for bare in bares:
            assert not bare

        bares = projectd_2.is_bare()["is_bare"].values
        for bare in bares:
            assert not bare

    def test_commit_history(self, local_project):
        projectd_1 = local_project["projectd_1"]

        ch = projectd_1.commit_history(branch="main")
        assert ch.shape[0] == 12

        ch2 = projectd_1.commit_history(branch="main", ignore_globs=["*.[!p][!y]"])
        assert ch2.shape[0] == 5

        ch3 = projectd_1.commit_history(branch="main", limit=4)
        assert ch3.shape[0] == 4

        ch4 = projectd_1.commit_history(branch="main", days=5)
        assert ch4.shape[0] == 12

    def test_file_change_history(self, local_project):
        projectd_1 = local_project["projectd_1"]

        fch = projectd_1.file_change_history(branch="main")
        assert fch.shape[0] == 12  # 2 READMEs + 5 py files + 5 js files

        fch2 = projectd_1.file_change_history(branch="main", ignore_globs=["*.[!p][!y]"])
        assert fch2.shape[0] == 5  # 5 py files

        fch4 = projectd_1.file_change_history(branch="main", ignore_globs=["*.[!j][!s]"])
        assert fch4.shape[0] == 5  # 5 js files

        fch3 = projectd_1.file_change_history(branch="main", limit=4)
        assert fch3.shape[0] == 4

    def test_file_change_rates(self, local_project):
        projectd_1 = local_project["projectd_1"]

        fcr = projectd_1.file_change_rates(branch="main")
        assert fcr.shape[0] == 12
        assert fcr["unique_committers"].sum() == 12
        assert fcr["net_change"].sum() == 17

    def test_has_coverage(self, local_project):
        projectd_1 = local_project["projectd_1"]

        # We know this repo doesn't have coverage
        coverages = projectd_1.has_coverage()["has_coverage"].values
        for coverage in coverages:
            assert not coverage

    def test_bus_factor(self, local_project):
        projectd_1 = local_project["projectd_1"]

        # We know this repo only has one committer
        bf = projectd_1.bus_factor(by="projectd")
        assert bf["bus factor"].values[0] == 1

    def test_blame(self, local_project):
        projectd_1 = local_project["projectd_1"]

        blame = projectd_1.blame(ignore_globs=["*.[!p][!y]"])
        assert blame["loc"].sum() == 10  # 5 files * 2 lines each
        assert blame.shape[0] == 1

    def test_cumulative_blame(self, local_project):
        projectd_1 = local_project["projectd_1"]

        cblame = projectd_1.cumulative_blame(by="committer", branch="main")
        assert cblame.shape[0] > 0  # Just check that we have some rows
        assert cblame[cblame.columns.values[0]].sum() > 0  # Check that we have some blame data

    def test_revs(self, local_project):
        projectd_1 = local_project["projectd_1"]

        revs = projectd_1.revs(branch="main", num_datapoints=2)
        assert revs.shape[0] == 2

        revs = projectd_1.revs(branch="main", limit=2)
        assert revs.shape[0] == 2

        revs = projectd_1.revs(branch="main")
        assert revs.shape[0] == 12  # 2 READMEs + 5 py files + 5 js files
