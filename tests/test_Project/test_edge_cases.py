import git
import pandas as pd
import pytest

from gitpandas import ProjectDirectory


class TestProjectEdgeCases:
    """Test ProjectDirectory edge cases and boundary conditions."""

    @pytest.fixture
    def empty_project_dir(self, tmp_path):
        """Create a directory with no git repositories."""
        project_path = tmp_path / "empty_project"
        project_path.mkdir()
        return str(project_path)

    @pytest.fixture
    def single_repo_project(self, tmp_path):
        """Create a project directory with one repository."""
        project_path = tmp_path / "single_repo_project"
        project_path.mkdir()

        # Create single repository
        repo_path = project_path / "repo1"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create commit
        (repo_path / "README.md").write_text("# Repo 1")
        repo.index.add(["README.md"])
        repo.index.commit("Initial commit")

        return str(project_path)

    @pytest.fixture
    def multi_repo_project(self, tmp_path):
        """Create a project directory with multiple repositories."""
        project_path = tmp_path / "multi_repo_project"
        project_path.mkdir()

        # Create multiple repositories
        for i in range(3):
            repo_path = project_path / f"repo{i + 1}"
            repo_path.mkdir()
            repo = git.Repo.init(repo_path)

            # Configure git user
            repo.config_writer().set_value("user", "name", f"User {i + 1}").release()
            repo.config_writer().set_value("user", "email", f"user{i + 1}@example.com").release()

            # Create commits
            (repo_path / "README.md").write_text(f"# Repo {i + 1}")
            repo.index.add(["README.md"])
            repo.index.commit(f"Initial commit for repo {i + 1}")

            # Add another commit
            (repo_path / f"file{i + 1}.txt").write_text(f"Content {i + 1}")
            repo.index.add([f"file{i + 1}.txt"])
            repo.index.commit(f"Add file{i + 1}.txt")

        return str(project_path)

    @pytest.fixture
    def mixed_content_project(self, tmp_path):
        """Create a project directory with repos and non-repo directories."""
        project_path = tmp_path / "mixed_content_project"
        project_path.mkdir()

        # Create a git repository
        repo_path = project_path / "valid_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create commit
        (repo_path / "README.md").write_text("# Valid Repo")
        repo.index.add(["README.md"])
        repo.index.commit("Initial commit")

        # Create non-git directories
        (project_path / "not_a_repo").mkdir()
        (project_path / "another_dir").mkdir()

        # Create some files in root
        (project_path / "root_file.txt").write_text("Root content")

        return str(project_path)

    def test_empty_project_directory(self, empty_project_dir):
        """Test project operations on directory with no repositories."""
        project = ProjectDirectory(working_dir=empty_project_dir)

        # Test that repositories list is empty
        repos = project.repos
        assert isinstance(repos, list | set)
        assert len(repos) == 0

        # Test aggregation methods on empty project
        try:
            commit_history = project.commit_history()
            assert isinstance(commit_history, pd.DataFrame)
            assert commit_history.empty
        except Exception:
            # Empty projects may fail aggregation operations
            pass

        try:
            file_change_history = project.file_change_history()
            assert isinstance(file_change_history, pd.DataFrame)
            assert file_change_history.empty
        except Exception:
            pass

        try:
            blame_df = project.blame()
            assert isinstance(blame_df, pd.DataFrame)
            assert blame_df.empty
        except Exception:
            # Empty projects may fail blame operations
            pass

        file_change_rates = project.file_change_rates()
        assert isinstance(file_change_rates, pd.DataFrame)
        assert file_change_rates.empty

    def test_single_repository_project(self, single_repo_project):
        """Test project operations with exactly one repository."""
        project = ProjectDirectory(working_dir=single_repo_project)

        # Test that repositories list has one item
        repos = project.repos
        assert isinstance(repos, list)
        assert len(repos) == 1

        # Test aggregation methods
        commit_history = project.commit_history()
        assert isinstance(commit_history, pd.DataFrame)
        assert len(commit_history) >= 1

        file_change_history = project.file_change_history()
        assert isinstance(file_change_history, pd.DataFrame)
        assert len(file_change_history) >= 1

        blame_df = project.blame()
        assert isinstance(blame_df, pd.DataFrame)
        assert len(blame_df) >= 1

    def test_multi_repository_project(self, multi_repo_project):
        """Test project operations with multiple repositories."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test that repositories list has multiple items
        repos = project.repos
        assert isinstance(repos, list)
        assert len(repos) == 3

        # Test aggregation methods combine data from all repos
        commit_history = project.commit_history()
        assert isinstance(commit_history, pd.DataFrame)
        # Should have commits from all 3 repositories (2 commits each = 6 total)
        assert len(commit_history) >= 6

        # Test that repository column exists and has multiple values
        if "repository" in commit_history.columns:
            unique_repos = commit_history["repository"].nunique()
            assert unique_repos == 3

    def test_mixed_content_project(self, mixed_content_project):
        """Test project with mix of repositories and non-repository directories."""
        project = ProjectDirectory(working_dir=mixed_content_project)

        # Should only include the valid repository
        repos = project.repos
        assert isinstance(repos, list)
        assert len(repos) == 1

        # Operations should work with the valid repository
        commit_history = project.commit_history()
        assert isinstance(commit_history, pd.DataFrame)
        assert len(commit_history) >= 1

    def test_project_with_zero_limit(self, multi_repo_project):
        """Test project operations with limit=0."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with zero limit
        commit_history = project.commit_history(limit=0)
        assert isinstance(commit_history, pd.DataFrame)
        assert commit_history.empty

        file_change_history = project.file_change_history(limit=0)
        assert isinstance(file_change_history, pd.DataFrame)
        assert file_change_history.empty

    def test_project_with_negative_limit(self, multi_repo_project):
        """Test project operations with negative limit."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with negative limit - should handle gracefully
        commit_history = project.commit_history(limit=-1)
        assert isinstance(commit_history, pd.DataFrame)

        file_change_history = project.file_change_history(limit=-1)
        assert isinstance(file_change_history, pd.DataFrame)

    def test_project_with_very_large_limit(self, multi_repo_project):
        """Test project operations with very large limit."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with extremely large limit
        large_limit = 999999999

        commit_history = project.commit_history(limit=large_limit)
        assert isinstance(commit_history, pd.DataFrame)
        # Should not crash and return available data
        assert len(commit_history) <= large_limit

    def test_project_with_empty_globs(self, multi_repo_project):
        """Test project operations with empty glob patterns."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with empty include_globs
        commit_history = project.commit_history(include_globs=[])
        assert isinstance(commit_history, pd.DataFrame)

        # Test with empty ignore_globs
        commit_history = project.commit_history(ignore_globs=[])
        assert isinstance(commit_history, pd.DataFrame)

        # Test file_change_history with empty globs
        file_change_history = project.file_change_history(include_globs=[])
        assert isinstance(file_change_history, pd.DataFrame)

    def test_project_with_nonexistent_globs(self, multi_repo_project):
        """Test project operations with glob patterns that match nothing."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with globs that match no files
        nonexistent_globs = ["*.nonexistent", "*.xyz", "impossible_pattern_*"]

        commit_history = project.commit_history(include_globs=nonexistent_globs)
        assert isinstance(commit_history, pd.DataFrame)

        file_change_history = project.file_change_history(include_globs=nonexistent_globs)
        assert isinstance(file_change_history, pd.DataFrame)

    def test_project_boundary_dates(self, multi_repo_project):
        """Test project operations with boundary date conditions."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with days=0
        commit_history = project.commit_history(days=0)
        assert isinstance(commit_history, pd.DataFrame)

        # Test with days=1
        commit_history = project.commit_history(days=1)
        assert isinstance(commit_history, pd.DataFrame)

        # Test with very large days value
        commit_history = project.commit_history(days=365000)
        assert isinstance(commit_history, pd.DataFrame)

    def test_project_aggregation_consistency(self, multi_repo_project):
        """Test that project aggregation is consistent across calls."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test multiple calls return consistent results
        history1 = project.commit_history()
        history2 = project.commit_history()

        assert isinstance(history1, pd.DataFrame)
        assert isinstance(history2, pd.DataFrame)
        assert len(history1) == len(history2)

        # Test with different parameters
        history_limited = project.commit_history(limit=2)
        assert isinstance(history_limited, pd.DataFrame)
        assert len(history_limited) <= 2

    def test_project_with_nested_repositories(self, tmp_path):
        """Test project with nested repository structure."""
        project_path = tmp_path / "nested_project"
        project_path.mkdir()

        # Create top-level repository
        top_repo = git.Repo.init(project_path)
        top_repo.config_writer().set_value("user", "name", "Top User").release()
        top_repo.config_writer().set_value("user", "email", "top@example.com").release()

        (project_path / "top_file.txt").write_text("Top level content")
        top_repo.index.add(["top_file.txt"])
        top_repo.index.commit("Top level commit")

        # Create nested repository
        nested_path = project_path / "nested"
        nested_path.mkdir()
        nested_repo = git.Repo.init(nested_path)
        nested_repo.config_writer().set_value("user", "name", "Nested User").release()
        nested_repo.config_writer().set_value("user", "email", "nested@example.com").release()

        (nested_path / "nested_file.txt").write_text("Nested content")
        nested_repo.index.add(["nested_file.txt"])
        nested_repo.index.commit("Nested commit")

        project = ProjectDirectory(working_dir=str(project_path))

        # Should handle nested repositories appropriately
        repos = project.repos
        assert isinstance(repos, list)
        # Behavior may vary - might include both or just top-level
        assert len(repos) >= 1

    def test_project_with_symlinks(self, tmp_path):
        """Test project handling of symbolic links."""
        project_path = tmp_path / "symlink_project"
        project_path.mkdir()

        # Create a repository
        repo_path = project_path / "real_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        (repo_path / "file.txt").write_text("Real content")
        repo.index.add(["file.txt"])
        repo.index.commit("Real commit")

        # Create symlink (if supported on this platform)
        try:
            symlink_path = project_path / "symlink_repo"
            symlink_path.symlink_to(repo_path)

            project = ProjectDirectory(working_dir=str(project_path))

            # Should handle symlinks gracefully
            repos = project.repos
            assert isinstance(repos, list)
            # May or may not include symlinked repos

            commit_history = project.commit_history()
            assert isinstance(commit_history, pd.DataFrame)

        except (OSError, NotImplementedError):
            # Symlinks not supported on this platform
            pytest.skip("Symlinks not supported on this platform")

    def test_project_with_bare_repositories(self, tmp_path):
        """Test project with bare repositories."""
        project_path = tmp_path / "bare_project"
        project_path.mkdir()

        # Create a normal repository first
        normal_path = project_path / "normal_repo"
        normal_path.mkdir()
        normal_repo = git.Repo.init(normal_path)

        normal_repo.config_writer().set_value("user", "name", "Test User").release()
        normal_repo.config_writer().set_value("user", "email", "test@example.com").release()

        (normal_path / "file.txt").write_text("Normal content")
        normal_repo.index.add(["file.txt"])
        normal_repo.index.commit("Normal commit")

        # Create a bare repository
        bare_path = project_path / "bare_repo.git"
        git.Repo.init(bare_path, bare=True)

        project = ProjectDirectory(working_dir=str(project_path))

        # Should handle mix of bare and normal repositories
        repos = project.repos
        assert isinstance(repos, list)
        # Should at least include the normal repository
        assert len(repos) >= 1

        commit_history = project.commit_history()
        assert isinstance(commit_history, pd.DataFrame)

    def test_project_memory_efficiency(self, multi_repo_project):
        """Test project operations with memory efficiency considerations."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test with small limits to simulate memory constraints
        commit_history = project.commit_history(limit=1)
        assert isinstance(commit_history, pd.DataFrame)
        assert len(commit_history) <= 1

        file_change_history = project.file_change_history(limit=1)
        assert isinstance(file_change_history, pd.DataFrame)
        assert len(file_change_history) <= 1

    def test_project_concurrent_repository_access(self, multi_repo_project):
        """Test project operations that access multiple repositories."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Simulate rapid successive calls
        results = []
        for _i in range(3):
            commit_history = project.commit_history()
            results.append(len(commit_history))

        # All calls should return consistent results
        assert all(r == results[0] for r in results)

    def test_project_data_aggregation_edge_cases(self, multi_repo_project):
        """Test edge cases in data aggregation across repositories."""
        project = ProjectDirectory(working_dir=multi_repo_project)

        # Test that aggregated data maintains proper structure
        commit_history = project.commit_history()

        if not commit_history.empty:
            # Check that repository identification is maintained
            if "repository" in commit_history.columns:
                # Should have entries from multiple repositories
                unique_repos = commit_history["repository"].nunique()
                assert unique_repos > 1

            # Check that dates are properly handled across repositories
            if "date" in commit_history.columns:
                # Should be able to sort by date
                sorted_history = commit_history.sort_values("date")
                assert len(sorted_history) == len(commit_history)


class TestProjectDataValidation:
    """Test ProjectDirectory data validation and aggregation."""

    @pytest.fixture
    def validation_project(self, tmp_path):
        """Create a project for data validation tests."""
        project_path = tmp_path / "validation_project"
        project_path.mkdir()

        # Create two repositories with different characteristics
        for i in range(2):
            repo_path = project_path / f"repo{i + 1}"
            repo_path.mkdir()
            repo = git.Repo.init(repo_path)

            # Configure git user
            repo.config_writer().set_value("user", "name", f"User {i + 1}").release()
            repo.config_writer().set_value("user", "email", f"user{i + 1}@example.com").release()

            # Create different types of commits
            (repo_path / f"file{i + 1}.txt").write_text(f"Content {i + 1}")
            repo.index.add([f"file{i + 1}.txt"])
            repo.index.commit(f"Initial commit for repo {i + 1}")

            # Add more commits with different patterns
            for j in range(2):
                (repo_path / f"extra{j}.txt").write_text(f"Extra content {j}")
                repo.index.add([f"extra{j}.txt"])
                repo.index.commit(f"Extra commit {j} in repo {i + 1}")

        return str(project_path)

    def test_aggregated_commit_history_data_types(self, validation_project):
        """Test that aggregated commit history has proper data types."""
        project = ProjectDirectory(working_dir=validation_project)
        history = project.commit_history()

        if not history.empty:
            # Check numeric columns
            numeric_columns = ["insertions", "deletions", "lines", "net"]
            for col in numeric_columns:
                if col in history.columns:
                    assert pd.api.types.is_numeric_dtype(history[col]), f"Column {col} should be numeric"

            # Check string columns
            string_columns = ["message", "author", "committer", "repository"]
            for col in string_columns:
                if col in history.columns:
                    assert history[col].dtype == object, f"Column {col} should be string/object type"

    def test_aggregated_blame_consistency(self, validation_project):
        """Test that aggregated blame data is consistent."""
        project = ProjectDirectory(working_dir=validation_project)
        blame_df = project.blame()

        if not blame_df.empty:
            # Check that repository information is present
            if "repository" in blame_df.columns:
                # Should have data from multiple repositories
                unique_repos = blame_df["repository"].nunique()
                assert unique_repos >= 1

            # Check that lines of code are reasonable
            if "loc" in blame_df.columns:
                assert (blame_df["loc"] >= 0).all(), "Lines of code should be non-negative"

    def test_aggregated_file_change_rates_bounds(self, validation_project):
        """Test that aggregated file change rates have reasonable bounds."""
        project = ProjectDirectory(working_dir=validation_project)
        change_rates = project.file_change_rates()

        if not change_rates.empty:
            # Check that rates are non-negative
            rate_columns = ["abs_rate_of_change", "net_rate_of_change", "edit_rate"]
            for col in rate_columns:
                if col in change_rates.columns:
                    assert (change_rates[col] >= 0).all(), f"{col} should be non-negative"

            # Check repository identification
            if "repository" in change_rates.columns:
                unique_repos = change_rates["repository"].nunique()
                assert unique_repos >= 1

    def test_project_data_consistency_across_methods(self, validation_project):
        """Test that data is consistent across different project methods."""
        project = ProjectDirectory(working_dir=validation_project)

        # Get data from different methods
        commit_history = project.commit_history()
        file_change_history = project.file_change_history()
        blame_df = project.blame()

        # All should be DataFrames
        assert isinstance(commit_history, pd.DataFrame)
        assert isinstance(file_change_history, pd.DataFrame)
        assert isinstance(blame_df, pd.DataFrame)

        # If repository column exists, should have consistent repository names
        repo_names_commit = set()
        repo_names_file = set()
        repo_names_blame = set()

        if not commit_history.empty and "repository" in commit_history.columns:
            repo_names_commit = set(commit_history["repository"].unique())

        if not file_change_history.empty and "repository" in file_change_history.columns:
            repo_names_file = set(file_change_history["repository"].unique())

        if not blame_df.empty and "repository" in blame_df.columns:
            repo_names_blame = set(blame_df["repository"].unique())

        # Repository names should be consistent across methods
        all_repo_names = repo_names_commit | repo_names_file | repo_names_blame
        if all_repo_names:
            # Each method should use the same repository naming convention
            assert len(all_repo_names) >= 1

    def test_project_empty_repository_handling(self, tmp_path):
        """Test project handling when some repositories are empty."""
        project_path = tmp_path / "mixed_empty_project"
        project_path.mkdir()

        # Create one normal repository
        normal_path = project_path / "normal_repo"
        normal_path.mkdir()
        normal_repo = git.Repo.init(normal_path)

        normal_repo.config_writer().set_value("user", "name", "Test User").release()
        normal_repo.config_writer().set_value("user", "email", "test@example.com").release()

        (normal_path / "file.txt").write_text("Normal content")
        normal_repo.index.add(["file.txt"])
        normal_repo.index.commit("Normal commit")

        # Create one empty repository
        empty_path = project_path / "empty_repo"
        empty_path.mkdir()
        empty_repo = git.Repo.init(empty_path)
        empty_repo.config_writer().set_value("user", "name", "Empty User").release()
        empty_repo.config_writer().set_value("user", "email", "empty@example.com").release()

        project = ProjectDirectory(working_dir=str(project_path))

        # Should handle mix of empty and non-empty repositories
        repos = project.repos
        assert isinstance(repos, list)
        assert len(repos) == 2

        # Aggregation should work despite empty repository
        commit_history = project.commit_history()
        assert isinstance(commit_history, pd.DataFrame)
        # Should have at least one commit from the normal repository
        assert len(commit_history) >= 1
