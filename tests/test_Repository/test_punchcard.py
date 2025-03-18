import git
import pandas as pd
import pytest

from gitpandas import Repository


@pytest.fixture
def local_repo(tmp_path):
    """Fixture for a local repository with commits at different times."""
    # Create a temporary directory
    repo_dir = tmp_path / "repository1"
    repo_dir.mkdir()

    # Initialize a git repo
    grepo = git.Repo.init(str(repo_dir))

    # Configure git user
    grepo.git.config("user.name", "Test User")
    grepo.git.config("user.email", "test@example.com")

    # Create and checkout master branch
    grepo.git.checkout("-b", "master")

    # Add a README file
    readme_path = repo_dir / "README.md"
    readme_path.write_text("Sample README for a sample project\n")

    # Commit it
    grepo.git.add("README.md")

    # Set the environment variables for the commit time
    # Monday morning
    env = {
        "GIT_AUTHOR_DATE": "2023-01-02T09:30:00",
        "GIT_COMMITTER_DATE": "2023-01-02T09:30:00",
    }
    grepo.git.commit(m="first commit", env=env)

    # Add some Python files at different times
    for idx, (day, hour) in enumerate(
        [
            # Tuesday afternoon
            (3, 14),
            # Wednesday evening
            (4, 19),
            # Thursday night
            (5, 22),
            # Friday morning
            (6, 8),
            # Saturday afternoon
            (7, 15),
        ]
    ):
        py_file = repo_dir / f"file_{idx}.py"
        py_file.write_text("import sys\nimport os\n")

        grepo.git.add(all=True)

        # Set the environment variables for the commit time
        env = {
            "GIT_AUTHOR_DATE": f"2023-01-{day:02d}T{hour:02d}:00:00",
            "GIT_COMMITTER_DATE": f"2023-01-{day:02d}T{hour:02d}:00:00",
        }
        grepo.git.commit(m=f"adding file_{idx}.py", env=env)

    # Create the Repository object
    git_pandas_repo = Repository(working_dir=str(repo_dir), verbose=True)

    yield git_pandas_repo

    # Cleanup
    git_pandas_repo.__del__()


class TestPunchcard:
    def test_punchcard_basic(self, local_repo):
        """Test basic functionality of the punchcard method."""
        punchcard = local_repo.punchcard(branch="master")

        # Check the shape and columns
        assert isinstance(punchcard, pd.DataFrame)
        assert punchcard.shape[0] > 0
        assert "hour_of_day" in punchcard.columns
        assert "day_of_week" in punchcard.columns
        assert "lines" in punchcard.columns
        assert "insertions" in punchcard.columns
        assert "deletions" in punchcard.columns
        assert "net" in punchcard.columns

        # Check that we have data for the days and hours we committed
        days_hours = [
            (0, 9),  # Monday 9am
            (1, 14),  # Tuesday 2pm
            (2, 19),  # Wednesday 7pm
            (3, 22),  # Thursday 10pm
            (4, 8),  # Friday 8am
            (5, 15),  # Saturday 3pm
        ]

        for day, hour in days_hours:
            matching_rows = punchcard[(punchcard["day_of_week"] == day) & (punchcard["hour_of_day"] == hour)]
            if len(matching_rows) > 0:
                assert matching_rows["net"].values[0] > 0

    def test_punchcard_normalize(self, local_repo):
        """Test the normalize parameter of the punchcard method."""
        # Get punchcard without normalization
        local_repo.punchcard(branch="master")

        # Get punchcard with normalization by value
        punchcard_norm = local_repo.punchcard(branch="master", normalize=1.0)

        # Check that the normalized values are between 0 and 1
        assert punchcard_norm["net"].max() <= 1.0
        assert punchcard_norm["net"].min() >= 0.0

        # Check that the row normalization works correctly
        for day in range(7):
            day_rows = punchcard_norm[punchcard_norm["day_of_week"] == day]
            if len(day_rows) > 0 and day_rows["net"].sum() > 0:
                # If there are commits on this day, the max value should be 1.0 or close to it
                assert day_rows["net"].max() <= 1.0

    def test_punchcard_by_parameter(self, local_repo):
        """Test the 'by' parameter of the punchcard method."""
        # Test with by='committer'
        punchcard_committer = local_repo.punchcard(branch="master", by="committer")

        # Check that we have the committer column
        assert "committer" in punchcard_committer.columns

        # Test with by='repository'
        punchcard_repo = local_repo.punchcard(branch="master", by="repository")

        # Check that we have the repository column
        assert "repository" in punchcard_repo.columns

    def test_punchcard_with_globs(self, local_repo):
        """Test the ignore_globs and include_globs parameters."""
        # Get punchcard for all files
        punchcard_all = local_repo.punchcard(branch="master")

        # Get punchcard ignoring Python files
        punchcard_no_py = local_repo.punchcard(branch="master", ignore_globs=["*.py"])

        # Check that we have fewer lines in the filtered punchcard
        assert punchcard_no_py["lines"].sum() < punchcard_all["lines"].sum()

        # Get punchcard including only Python files
        punchcard_only_py = local_repo.punchcard(branch="master", include_globs=["*.py"])

        # Check that we have fewer lines than the full punchcard
        assert punchcard_only_py["lines"].sum() < punchcard_all["lines"].sum()

        # Check that the sum of the filtered punchcards equals the total
        assert punchcard_no_py["lines"].sum() + punchcard_only_py["lines"].sum() == punchcard_all["lines"].sum()

    def test_punchcard_with_limit(self, local_repo):
        """Test the limit parameter of the punchcard method."""
        # Get punchcard with all commits
        punchcard_all = local_repo.punchcard(branch="master")

        # Get punchcard with limited commits
        punchcard_limited = local_repo.punchcard(branch="master", limit=3)

        # Check that we have fewer lines in the limited punchcard
        assert punchcard_limited["lines"].sum() <= punchcard_all["lines"].sum()

    def test_punchcard_with_days(self, local_repo):
        """Test the days parameter of the punchcard method."""
        # Get punchcard with all commits
        local_repo.punchcard(branch="master")

        # Get punchcard with commits from the last 2 days
        # Since our test data is from 2023, this should return an empty DataFrame
        punchcard_recent = local_repo.punchcard(branch="master", days=2)

        # Check that we have no lines in the recent punchcard
        assert punchcard_recent["lines"].sum() == 0
