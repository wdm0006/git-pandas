import git
import pandas as pd
import pytest

from gitpandas import Repository


@pytest.fixture
def local_repo(tmp_path):
    """Create a local git repository with various file types and structures."""
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()
    repo = git.Repo.init(repo_path)

    # Configure git user
    repo.config_writer().set_value("user", "name", "Test User").release()
    repo.config_writer().set_value("user", "email", "test@example.com").release()

    # Create and checkout master branch
    repo.git.checkout("-b", "master")

    # Create initial structure
    (repo_path / "src").mkdir()
    (repo_path / "docs").mkdir()
    (repo_path / "tests").mkdir()

    # Create various files
    files = {
        "README.md": "# Test Repository\nA test repository for gitpandas.",
        "src/main.py": "def main():\n    print('Hello, World!')\n    return True",
        "src/utils.py": "def helper():\n    return 'helper'",
        "docs/index.md": "# Documentation\nThis is the documentation.",
        "tests/test_main.py": "def test_main():\n    assert True",
        ".gitignore": "*.pyc\n__pycache__/\n.DS_Store",
    }

    # Create and commit files
    for path, content in files.items():
        file_path = repo_path / path
        file_path.write_text(content)
        repo.index.add([str(file_path)])

    repo.index.commit("Initial commit")

    # Create some ignored files
    (repo_path / "src/main.pyc").write_text("compiled python")
    (repo_path / "src/__pycache__").mkdir()
    (repo_path / "src/__pycache__/main.cpython-39.pyc").write_text("cached python")

    # Make a change to test commit content
    main_py = repo_path / "src/main.py"
    main_py.write_text("def main():\n    print('Hello, Universe!')\n    return True")
    repo.index.add([str(main_py)])
    commit = repo.index.commit("Update greeting")

    return {"repo_path": repo_path, "repo": Repository(working_dir=str(repo_path)), "last_commit": commit.hexsha}


class TestFileOperations:
    def test_list_files(self, local_repo):
        """Test listing files in the repository."""
        repo = local_repo["repo"]

        # Get all files
        files = repo.list_files()

        # Check basic DataFrame properties
        assert isinstance(files, pd.DataFrame)
        assert "file" in files.columns
        assert "mode" in files.columns
        assert "type" in files.columns
        assert "sha" in files.columns
        assert "repository" in files.columns

        # Check that we have the expected files
        file_paths = set(files["file"].values)
        expected_files = {
            "README.md",
            "src/main.py",
            "src/utils.py",
            "docs/index.md",
            "tests/test_main.py",
            ".gitignore",
        }
        assert file_paths == expected_files

        # Check that ignored files are not included
        assert "src/main.pyc" not in file_paths
        assert "src/__pycache__/main.cpython-39.pyc" not in file_paths

        # Check file types
        assert all(files["type"] == "blob")  # All should be files, not trees

        # Check file modes (should be regular files)
        assert all(files["mode"].isin(["100644"]))

    def test_get_file_content(self, local_repo):
        """Test getting file content from the repository."""
        repo = local_repo["repo"]

        # Test getting content of an existing file
        content = repo.get_file_content("src/main.py")
        assert content == "def main():\n    print('Hello, Universe!')\n    return True"

        # Test getting content at a specific revision (first commit)
        first_content = repo.get_file_content("src/main.py", rev="HEAD^")
        assert first_content == "def main():\n    print('Hello, World!')\n    return True"

        # Test getting content of a non-existent file
        assert repo.get_file_content("nonexistent.txt") is None

        # Test getting content of an ignored file
        assert repo.get_file_content("src/main.pyc") is None

        # Test getting content with invalid revision
        assert repo.get_file_content("src/main.py", rev="invalid_rev") is None

    def test_get_commit_content(self, local_repo):
        """Test getting detailed content changes from a commit."""
        repo = local_repo["repo"]
        commit_sha = local_repo["last_commit"]

        # Get changes from the last commit
        changes = repo.get_commit_content(commit_sha)

        # Check basic DataFrame properties
        assert isinstance(changes, pd.DataFrame)
        assert "file" in changes.columns
        assert "change_type" in changes.columns
        assert "old_line_num" in changes.columns
        assert "new_line_num" in changes.columns
        assert "content" in changes.columns
        assert "repository" in changes.columns

        # Check that we have the expected changes
        assert len(changes) > 0
        file_changes = changes[changes["file"] == "src/main.py"]
        assert len(file_changes) > 0

        # Check for removed line
        removed = file_changes[file_changes["old_line_num"].notna()]
        assert len(removed) == 1
        assert "Hello, World!" in removed.iloc[0]["content"]

        # Check for added line
        added = file_changes[file_changes["new_line_num"].notna()]
        assert len(added) == 1
        assert "Hello, Universe!" in added.iloc[0]["content"]

        # Test with glob filters
        # Should find no changes when excluding .py files
        filtered = repo.get_commit_content(commit_sha, ignore_globs=["*.py"])
        assert len(filtered) == 0

        # Should find changes when including only .py files
        filtered = repo.get_commit_content(commit_sha, include_globs=["*.py"])
        assert len(filtered) > 0

        # Test with invalid commit
        invalid = repo.get_commit_content("invalid_sha")
        assert len(invalid) == 0
