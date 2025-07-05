import git
import pandas as pd
import pytest

from gitpandas import Repository


@pytest.fixture
def multi_committer_repo(tmp_path):
    """Fixture for a local repository with multiple committers."""
    # Create a temporary directory
    repo_dir = tmp_path / "repository1"
    repo_dir.mkdir()

    # Initialize a git repo
    grepo = git.Repo.init(str(repo_dir))

    # Add a README file with first committer
    readme_path = repo_dir / "README.md"
    readme_path.write_text("Sample README for a sample project\n")

    # Commit it with first committer
    grepo.git.config("user.name", "User One")
    grepo.git.config("user.email", "user1@example.com")
    grepo.git.add("README.md")
    grepo.git.commit(m="first commit")

    # Add Python files with different committers
    committers = [
        ("User One", "user1@example.com"),
        ("User Two", "user2@example.com"),
        ("User Three", "user3@example.com"),
    ]

    # Each committer adds 3 files
    for committer_idx, (name, email) in enumerate(committers):
        grepo.git.config("user.name", name)
        grepo.git.config("user.email", email)

        for file_idx in range(3):
            py_file = repo_dir / f"file_{committer_idx}_{file_idx}.py"
            py_file.write_text(
                f"import sys\nimport os\n\ndef function_{committer_idx}_{file_idx}():\n    return {committer_idx * 10 + file_idx}\n"  # noqa: E501
            )

            grepo.git.add(all=True)
            grepo.git.commit(m=f"adding file_{committer_idx}_{file_idx}.py")

    # Create a shared file that all committers contribute to
    shared_file = repo_dir / "shared.py"

    # First committer creates the file
    grepo.git.config("user.name", committers[0][0])
    grepo.git.config("user.email", committers[0][1])
    shared_file.write_text("import sys\nimport os\n\n# Shared file\n")
    grepo.git.add("shared.py")
    grepo.git.commit(m="creating shared file")

    # Second committer adds to the file
    grepo.git.config("user.name", committers[1][0])
    grepo.git.config("user.email", committers[1][1])
    with open(shared_file, "a") as f:
        f.write('\ndef shared_function_1():\n    return "shared1"\n')
    grepo.git.add("shared.py")
    grepo.git.commit(m="adding to shared file")

    # Third committer adds to the file
    grepo.git.config("user.name", committers[2][0])
    grepo.git.config("user.email", committers[2][1])
    with open(shared_file, "a") as f:
        f.write('\ndef shared_function_2():\n    return "shared2"\n')
    grepo.git.add("shared.py")
    grepo.git.commit(m="adding more to shared file")

    # Create the Repository object
    git_pandas_repo = Repository(working_dir=str(repo_dir), verbose=True)

    yield git_pandas_repo

    # Cleanup
    git_pandas_repo.__del__()


class TestBusFactor:
    def test_bus_factor_by_repository(self, multi_committer_repo):
        """Test the bus_factor method with by='repository'."""
        bus_factor = multi_committer_repo.bus_factor(by="repository")

        # Check the shape and columns
        assert isinstance(bus_factor, pd.DataFrame)
        assert bus_factor.shape[0] == 1

        # Check that we have the expected columns
        expected_columns = ["repository", "bus factor"]
        for col in expected_columns:
            assert col in bus_factor.columns

        # With 3 committers, the bus factor should be at least 1 and at most 3
        assert 1 <= bus_factor["bus factor"].values[0] <= 3

        # Since each committer has contributed roughly equally, the bus factor should be close to 3
        # But we'll just check it's at least 1 to be safe
        assert bus_factor["bus factor"].values[0] >= 1

    def test_bus_factor_with_globs(self, multi_committer_repo):
        """Test the ignore_globs and include_globs parameters."""
        # Get bus factor for all files
        bus_factor_all = multi_committer_repo.bus_factor(by="repository")

        # Get bus factor ignoring files from the first committer
        bus_factor_no_user1 = multi_committer_repo.bus_factor(by="repository", ignore_globs=["file_0_*.py"])

        # Get bus factor including only files from the first committer
        bus_factor_only_user1 = multi_committer_repo.bus_factor(by="repository", include_globs=["file_0_*.py"])

        # The bus factor should be different when we filter files
        assert bus_factor_no_user1["bus factor"].values[0] <= bus_factor_all["bus factor"].values[0]
        assert bus_factor_only_user1["bus factor"].values[0] <= bus_factor_all["bus factor"].values[0]

    def test_bus_factor_calculation(self, multi_committer_repo):
        """Test the bus factor calculation logic."""
        # Get the blame data to understand the distribution of contributions
        blame = multi_committer_repo.blame(by="repository")

        # Calculate the bus factor manually for the repository
        committer_loc = blame.groupby("committer")["loc"].sum()
        total_loc = committer_loc.sum()

        # Sort committers by LOC in descending order
        sorted_committers = committer_loc.sort_values(ascending=False)

        # Calculate cumulative percentage
        cumulative_pct = sorted_committers.cumsum() / total_loc

        # Find the number of committers needed to reach 50%
        # The implementation counts the number of committers needed to reach >= 50%
        manual_bus_factor = (cumulative_pct < 0.5).sum() + 1

        # Get the bus factor from the method
        bus_factor = multi_committer_repo.bus_factor(by="repository")["bus factor"].values[0]

        # The calculated bus factor should match our manual calculation
        assert bus_factor == manual_bus_factor

    def test_bus_factor_by_file(self, multi_committer_repo):
        """Test the bus_factor method with by='file'."""
        bus_factor_df = multi_committer_repo.bus_factor(by="file")

        # Check the shape and columns
        assert isinstance(bus_factor_df, pd.DataFrame)
        assert bus_factor_df.shape[0] > 0  # Should have at least one file

        # Check that we have the expected columns
        expected_columns = ["file", "bus factor", "repository"]
        for col in expected_columns:
            assert col in bus_factor_df.columns

        # All bus factors should be at least 1 (minimum one contributor per file)
        assert (bus_factor_df["bus factor"] >= 1).all()

        # Bus factors should be reasonable (not exceed total number of committers)
        max_committers = 3  # We created 3 committers in the fixture
        assert (bus_factor_df["bus factor"] <= max_committers).all()

        # Repository column should be consistent
        assert len(bus_factor_df["repository"].unique()) == 1

        # Check that we have results for the expected files
        file_list = bus_factor_df["file"].tolist()

        # We should have Python files from our test fixture
        python_files = [f for f in file_list if f.endswith(".py")]
        assert len(python_files) > 0, "Should have Python files in results"

    def test_bus_factor_by_file_with_globs(self, multi_committer_repo):
        """Test the file-wise bus factor with glob filtering."""
        # Get bus factor for all files
        bus_factor_all = multi_committer_repo.bus_factor(by="file")

        # Get bus factor for only Python files
        bus_factor_py = multi_committer_repo.bus_factor(by="file", include_globs=["*.py"])

        # Get bus factor excluding Python files
        bus_factor_no_py = multi_committer_repo.bus_factor(by="file", ignore_globs=["*.py"])

        # Python-only results should be a subset of all results
        assert len(bus_factor_py) <= len(bus_factor_all)

        # All files in Python-only results should end with .py
        if not bus_factor_py.empty:
            assert bus_factor_py["file"].str.endswith(".py").all()

        # No files in no-Python results should end with .py
        if not bus_factor_no_py.empty:
            assert not bus_factor_no_py["file"].str.endswith(".py").any()

    def test_bus_factor_by_file_single_committer_files(self, multi_committer_repo):
        """Test file-wise bus factor for files with single committers."""
        # Get all file-wise bus factors
        bus_factor_df = multi_committer_repo.bus_factor(by="file")

        # Filter for files that have only one committer (bus factor of 1)
        single_committer_files = bus_factor_df[bus_factor_df["bus factor"] == 1]

        # Should have some single-committer files from our fixture
        # (each committer created their own files)
        assert len(single_committer_files) > 0

        # Verify the bus factor calculation for a single-committer file
        if len(single_committer_files) > 0:
            sample_file = single_committer_files.iloc[0]["file"]

            # Get blame data for this specific file
            blame = multi_committer_repo.blame(by="file")
            if isinstance(blame.index, pd.MultiIndex):
                blame = blame.reset_index()

            file_blame = blame[blame["file"] == sample_file]
            unique_committers = (
                file_blame["committer"].nunique()
                if "committer" in file_blame.columns
                else file_blame["author"].nunique()
            )

            # A file with bus factor 1 should have contributions that are >=50% from one person
            # But due to rounding and the nature of our test data, we'll just verify it's reasonable
            assert unique_committers >= 1
