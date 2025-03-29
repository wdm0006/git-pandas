import git
import numpy as np
import pandas as pd
import pytest

from gitpandas import Repository


@pytest.fixture
def local_repo(tmp_path):
    """Fixture for a local repository with different file types."""
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
    grepo.git.commit(m="first commit")

    # Add some Python files with different content
    for idx in range(3):
        py_file = repo_dir / f"file_{idx}.py"
        py_file.write_text(f"import sys\nimport os\n\ndef function_{idx}():\n    return {idx}\n")

        grepo.git.add(all=True)
        grepo.git.commit(m=f"adding file_{idx}.py")

    # Add a JavaScript file
    js_file = repo_dir / "script.js"
    js_file.write_text('function hello() {\n    console.log("Hello, world!");\n}\n')

    grepo.git.add("script.js")
    grepo.git.commit(m="adding script.js")

    # Add a CSS file
    css_file = repo_dir / "style.css"
    css_file.write_text("body {\n    margin: 0;\n    padding: 0;\n}\n")

    grepo.git.add("style.css")
    grepo.git.commit(m="adding style.css")

    # Create a subdirectory
    subdir = repo_dir / "subdir"
    subdir.mkdir()

    # Add a file in the subdirectory
    subdir_file = subdir / "subfile.py"
    subdir_file.write_text('import sys\n\ndef subfunction():\n    return "sub"\n')

    grepo.git.add(all=True)
    grepo.git.commit(m="adding subdir/subfile.py")

    # Create the Repository object
    git_pandas_repo = Repository(working_dir=str(repo_dir), verbose=True)

    yield git_pandas_repo

    # Cleanup
    git_pandas_repo.__del__()


class TestFileDetail:
    def test_file_detail_basic(self, local_repo):
        """Test basic functionality of the file_detail method."""
        file_detail = local_repo.file_detail()

        # Check the shape and columns
        assert isinstance(file_detail, pd.DataFrame)
        assert file_detail.shape[0] > 0

        # Check that we have the expected columns
        expected_columns = ["loc", "file_owner", "ext", "last_edit_date", "repository"]
        for col in expected_columns:
            assert col in file_detail.columns

        # Check that we have entries for each file type
        exts = file_detail["ext"].unique()
        assert "md" in exts
        assert "py" in exts
        assert "js" in exts
        assert "css" in exts

        # Check that the LOC counts are correct for different file types
        md_loc = file_detail.loc[file_detail["ext"] == "md", "loc"].sum()
        py_loc = file_detail.loc[file_detail["ext"] == "py", "loc"].sum()
        js_loc = file_detail.loc[file_detail["ext"] == "js", "loc"].sum()
        css_loc = file_detail.loc[file_detail["ext"] == "css", "loc"].sum()

        assert md_loc == 1  # README.md has 1 line
        assert py_loc == 19  # 3 Python files with 5 lines each + 1 with 4 lines
        assert js_loc == 3  # script.js has 3 lines
        assert css_loc == 4  # style.css has 4 lines

    def test_file_detail_with_globs(self, local_repo):
        """Test the ignore_globs and include_globs parameters."""
        # Get file detail for all files
        file_detail_all = local_repo.file_detail()

        # Get file detail ignoring Python files
        file_detail_no_py = local_repo.file_detail(ignore_globs=["*.py"])

        # Check that we have fewer files in the filtered file detail
        assert file_detail_no_py.shape[0] < file_detail_all.shape[0]

        # Check that no Python files are included
        assert "py" not in file_detail_no_py["ext"].values

        # Get file detail including only Python files
        file_detail_only_py = local_repo.file_detail(include_globs=["*.py"])

        # Check that we have fewer files than the full file detail
        assert file_detail_only_py.shape[0] < file_detail_all.shape[0]

        # Check that only Python files are included
        for ext in file_detail_only_py["ext"].values:
            assert ext == "py"

        # Check that the sum of the filtered file details equals the total
        assert file_detail_no_py.shape[0] + file_detail_only_py.shape[0] == file_detail_all.shape[0]

    def test_file_detail_with_rev(self, local_repo):
        """Test the rev parameter of the file_detail method."""
        # Get file detail for the current revision
        file_detail_head = local_repo.file_detail(rev="HEAD")

        # Get file detail for the first commit
        # This should only include the README.md file
        first_commit = local_repo.revs(branch="master").iloc[-1]["rev"]
        file_detail_first = local_repo.file_detail(rev=first_commit)

        # Check that we have fewer files in the first commit
        assert file_detail_first.shape[0] < file_detail_head.shape[0]

        # Check that only md extension is included in the first commit
        assert file_detail_first.shape[0] == 1
        assert "md" in file_detail_first["ext"].values

    def test_file_detail_committer(self, local_repo):
        """Test the committer parameter of the file_detail method."""
        # Get file detail with committer=True
        file_detail_committer = local_repo.file_detail(committer=True)

        # Check that we have the file_owner column
        assert "file_owner" in file_detail_committer.columns

        # Check that all file owners are 'Test User'
        for owner in file_detail_committer["file_owner"].values:
            assert owner["name"] == "Test User"

        # Get file detail with committer=False
        file_detail_no_committer = local_repo.file_detail(committer=False)

        # Check that we have the file_owner column
        assert "file_owner" in file_detail_no_committer.columns

        # Check that all file owners are 'Test User'
        for owner in file_detail_no_committer["file_owner"].values:
            assert owner["name"] == "Test User"

    def test_file_detail_last_edit(self, local_repo):
        """Test that the last_edit_date column contains valid timestamps."""
        file_detail = local_repo.file_detail()

        # Check that all last_edit_date values are datetime64 objects
        for last_edit in file_detail["last_edit_date"].values:
            assert isinstance(last_edit, np.datetime64 | pd.Timestamp)

        # Check that the last file added has the most recent timestamp
        # Find the file with the py extension in the subdir directory
        subdir_files = [idx for idx, ext in enumerate(file_detail["ext"].values) if ext == "py"]
        last_edit_dates = [file_detail["last_edit_date"].values[idx] for idx in subdir_files]
        last_edit = max(last_edit_dates)

        # All other files should have earlier or equal timestamps
        for edit_time in file_detail["last_edit_date"].values:
            assert edit_time <= last_edit
