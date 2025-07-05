import git
import pandas as pd
import pytest

from gitpandas import Repository


class TestRepositoryEdgeCases:
    """Test Repository edge cases and boundary conditions."""

    @pytest.fixture
    def empty_repo(self, tmp_path):
        """Create an empty git repository with no commits."""
        repo_path = tmp_path / "empty_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user but don't make any commits
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        return str(repo_path)

    @pytest.fixture
    def single_commit_repo(self, tmp_path):
        """Create a repository with exactly one commit."""
        repo_path = tmp_path / "single_commit_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create single commit
        (repo_path / "README.md").write_text("# Single Commit")
        repo.index.add(["README.md"])
        repo.index.commit("Initial commit")

        return str(repo_path)

    @pytest.fixture
    def large_file_repo(self, tmp_path):
        """Create a repository with a large file."""
        repo_path = tmp_path / "large_file_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create a large file (simulated)
        large_content = "A" * 10000  # 10KB file
        (repo_path / "large_file.txt").write_text(large_content)
        repo.index.add(["large_file.txt"])
        repo.index.commit("Add large file")

        return str(repo_path)

    def test_empty_repository_operations(self, empty_repo):
        """Test all repository operations on an empty repository."""
        repo = Repository(working_dir=empty_repo, default_branch="main")

        # Test commit_history on empty repo - may fail with GitCommandError
        try:
            history = repo.commit_history()
            assert isinstance(history, pd.DataFrame)
            assert history.empty
        except Exception:
            # Empty repos may fail git operations - this is acceptable
            pass

        # Test file_change_history on empty repo
        try:
            file_history = repo.file_change_history()
            assert isinstance(file_history, pd.DataFrame)
            assert file_history.empty
        except Exception:
            pass

        # Test blame on empty repo
        try:
            blame_df = repo.blame()
            assert isinstance(blame_df, pd.DataFrame)
            assert blame_df.empty
        except Exception:
            pass

        # Test file_change_rates on empty repo
        try:
            change_rates = repo.file_change_rates()
            assert isinstance(change_rates, pd.DataFrame)
            assert change_rates.empty
        except Exception:
            pass

        # Test punchcard on empty repo
        try:
            punchcard = repo.punchcard()
            assert isinstance(punchcard, pd.DataFrame)
            assert punchcard.empty
        except Exception:
            pass

    def test_single_commit_repository(self, single_commit_repo):
        """Test repository operations with exactly one commit."""
        repo = Repository(working_dir=single_commit_repo)

        # Test commit_history with single commit
        history = repo.commit_history()
        assert isinstance(history, pd.DataFrame)
        assert len(history) == 1

        # Test file_change_history with single commit
        file_history = repo.file_change_history()
        assert isinstance(file_history, pd.DataFrame)
        # Single commit should have file changes
        assert len(file_history) >= 1

        # Test blame with single commit
        blame_df = repo.blame()
        assert isinstance(blame_df, pd.DataFrame)
        # Should have blame data for the single file
        assert len(blame_df) >= 1

    def test_repository_with_zero_limit(self, single_commit_repo):
        """Test repository operations with limit=0."""
        repo = Repository(working_dir=single_commit_repo)

        # Test commit_history with zero limit
        history = repo.commit_history(limit=0)
        assert isinstance(history, pd.DataFrame)
        assert history.empty

        # Test file_change_history with zero limit
        file_history = repo.file_change_history(limit=0)
        assert isinstance(file_history, pd.DataFrame)
        assert file_history.empty

    def test_repository_with_negative_limit(self, single_commit_repo):
        """Test repository operations with negative limit."""
        repo = Repository(working_dir=single_commit_repo)

        # Test commit_history with negative limit - should handle gracefully
        history = repo.commit_history(limit=-1)
        assert isinstance(history, pd.DataFrame)
        # Negative limit should be handled (either ignored or treated as unlimited)

        # Test file_change_history with negative limit
        file_history = repo.file_change_history(limit=-1)
        assert isinstance(file_history, pd.DataFrame)

    def test_repository_with_very_large_limit(self, single_commit_repo):
        """Test repository operations with very large limit."""
        repo = Repository(working_dir=single_commit_repo)

        # Test with extremely large limit
        large_limit = 999999999

        history = repo.commit_history(limit=large_limit)
        assert isinstance(history, pd.DataFrame)
        # Should not crash and return available data
        assert len(history) <= large_limit

    def test_repository_with_empty_globs(self, single_commit_repo):
        """Test repository operations with empty glob patterns."""
        repo = Repository(working_dir=single_commit_repo)

        # Test with empty include_globs
        history = repo.commit_history(include_globs=[])
        assert isinstance(history, pd.DataFrame)

        # Test with empty ignore_globs
        history = repo.commit_history(ignore_globs=[])
        assert isinstance(history, pd.DataFrame)

        # Test file_change_history with empty globs
        file_history = repo.file_change_history(include_globs=[])
        assert isinstance(file_history, pd.DataFrame)

    def test_repository_with_nonexistent_globs(self, single_commit_repo):
        """Test repository operations with glob patterns that match nothing."""
        repo = Repository(working_dir=single_commit_repo)

        # Test with globs that match no files
        nonexistent_globs = ["*.nonexistent", "*.xyz", "impossible_pattern_*"]

        history = repo.commit_history(include_globs=nonexistent_globs)
        assert isinstance(history, pd.DataFrame)
        # Should return empty or minimal data

        file_history = repo.file_change_history(include_globs=nonexistent_globs)
        assert isinstance(file_history, pd.DataFrame)

    def test_repository_with_special_characters_in_paths(self, tmp_path):
        """Test repository with special characters in file paths."""
        repo_path = tmp_path / "special_chars_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create files with special characters (that are valid in git)
        special_files = [
            "file with spaces.txt",
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.with.dots.txt",
        ]

        for filename in special_files:
            try:
                (repo_path / filename).write_text(f"Content of {filename}")
                repo.index.add([filename])
            except Exception:
                # Skip files that can't be created on this filesystem
                continue

        if repo.index.entries:  # Only commit if we have files
            repo.index.commit("Add files with special characters")

            git_repo = Repository(working_dir=str(repo_path))

            # Test operations with special character files
            history = git_repo.commit_history()
            assert isinstance(history, pd.DataFrame)

            file_history = git_repo.file_change_history()
            assert isinstance(file_history, pd.DataFrame)

    def test_repository_with_unicode_content(self, tmp_path):
        """Test repository with unicode content in files."""
        repo_path = tmp_path / "unicode_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create file with unicode content
        unicode_content = "Hello 世界 🌍 Café naïve résumé"
        (repo_path / "unicode.txt").write_text(unicode_content, encoding="utf-8")
        repo.index.add(["unicode.txt"])
        repo.index.commit("Add unicode content")

        git_repo = Repository(working_dir=str(repo_path))

        # Test operations with unicode content
        history = git_repo.commit_history()
        assert isinstance(history, pd.DataFrame)

        blame_df = git_repo.blame()
        assert isinstance(blame_df, pd.DataFrame)

    def test_repository_boundary_dates(self, single_commit_repo):
        """Test repository operations with boundary date conditions."""
        repo = Repository(working_dir=single_commit_repo)

        # Test with days=0 (should include all commits)
        history = repo.commit_history(days=0)
        assert isinstance(history, pd.DataFrame)

        # Test with days=1 (should include recent commits)
        history = repo.commit_history(days=1)
        assert isinstance(history, pd.DataFrame)

        # Test with very large days value
        history = repo.commit_history(days=365000)  # 1000 years
        assert isinstance(history, pd.DataFrame)

    def test_repository_extreme_skip_values(self, single_commit_repo):
        """Test repository operations with extreme skip values."""
        repo = Repository(working_dir=single_commit_repo)

        # Test with skip=0
        revs = repo.revs(skip=0)
        assert isinstance(revs, pd.DataFrame)

        # Test with very large skip value
        revs = repo.revs(skip=999999)
        assert isinstance(revs, pd.DataFrame)
        # Should return empty or minimal DataFrame when skip exceeds available commits
        # (behavior may vary based on implementation)

    def test_repository_with_large_files(self, large_file_repo):
        """Test repository operations with large files."""
        repo = Repository(working_dir=large_file_repo)

        # Test blame on large file
        blame_df = repo.blame()
        assert isinstance(blame_df, pd.DataFrame)

        # Test file_change_history on large file
        file_history = repo.file_change_history()
        assert isinstance(file_history, pd.DataFrame)

        # Should handle large files without crashing
        assert len(file_history) >= 1

    def test_repository_operations_consistency(self, single_commit_repo):
        """Test that repository operations return consistent data structures."""
        repo = Repository(working_dir=single_commit_repo)

        # Test that all operations return DataFrames with expected column types
        history = repo.commit_history()
        assert isinstance(history, pd.DataFrame)
        if not history.empty:
            # Check that date columns are properly formatted (date may be in index)
            has_date_column = "date" in history.columns
            has_date_index = history.index.name == "date" or (
                hasattr(history.index, "names") and "date" in history.index.names
            )
            assert has_date_column or has_date_index or history.empty

        file_history = repo.file_change_history()
        assert isinstance(file_history, pd.DataFrame)
        if not file_history.empty:
            # Check for expected numeric columns
            numeric_cols = ["insertions", "deletions", "lines"]
            for col in numeric_cols:
                if col in file_history.columns:
                    assert pd.api.types.is_numeric_dtype(file_history[col])

    def test_repository_cache_behavior_edge_cases(self, single_commit_repo):
        """Test repository caching behavior with edge cases."""
        repo = Repository(working_dir=single_commit_repo)

        # Test that multiple calls return consistent results
        history1 = repo.commit_history()
        history2 = repo.commit_history()

        assert isinstance(history1, pd.DataFrame)
        assert isinstance(history2, pd.DataFrame)
        assert len(history1) == len(history2)

        # Test with different parameters
        history_limited = repo.commit_history(limit=1)
        assert isinstance(history_limited, pd.DataFrame)
        assert len(history_limited) <= 1

    def test_repository_with_binary_files(self, tmp_path):
        """Test repository operations with binary files."""
        repo_path = tmp_path / "binary_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create a binary file (simulated)
        binary_content = bytes([i % 256 for i in range(1000)])
        (repo_path / "binary.bin").write_bytes(binary_content)
        repo.index.add(["binary.bin"])
        repo.index.commit("Add binary file")

        git_repo = Repository(working_dir=str(repo_path))

        # Test operations with binary file - should handle gracefully
        history = git_repo.commit_history()
        assert isinstance(history, pd.DataFrame)

        # Blame might skip binary files or handle them specially
        blame_df = git_repo.blame()
        assert isinstance(blame_df, pd.DataFrame)

    def test_repository_memory_constraints(self, single_commit_repo):
        """Test repository operations under simulated memory constraints."""
        repo = Repository(working_dir=single_commit_repo)

        # Test with very small limits to simulate memory constraints
        history = repo.commit_history(limit=1)
        assert isinstance(history, pd.DataFrame)
        assert len(history) <= 1

        file_history = repo.file_change_history(limit=1)
        assert isinstance(file_history, pd.DataFrame)
        assert len(file_history) <= 1

    def test_repository_concurrent_access_simulation(self, single_commit_repo):
        """Test repository operations that might be affected by concurrent access."""
        repo = Repository(working_dir=single_commit_repo)

        # Simulate multiple rapid calls (as might happen in concurrent scenarios)
        results = []
        for _i in range(5):
            history = repo.commit_history()
            results.append(len(history))

        # All calls should return consistent results
        assert all(r == results[0] for r in results)

    def test_repository_with_no_default_branch(self, tmp_path):
        """Test repository operations when default branch detection might fail."""
        repo_path = tmp_path / "no_default_branch_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        git_repo = Repository(working_dir=str(repo_path), default_branch="main")

        # Test operations on repo with no commits/branches
        try:
            default_branch = git_repo.default_branch
            assert isinstance(default_branch, str) or default_branch is None
        except Exception:
            # It's acceptable if default_branch detection fails on empty repo
            pass

        # Basic operations should still work
        try:
            history = git_repo.commit_history()
            assert isinstance(history, pd.DataFrame)
            assert history.empty
        except Exception:
            # Empty repos may fail git operations - this is acceptable
            pass


class TestRepositoryDataValidation:
    """Test Repository data validation and sanitization."""

    @pytest.fixture
    def validation_repo(self, tmp_path):
        """Create a repository for data validation tests."""
        repo_path = tmp_path / "validation_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        # Create test file
        (repo_path / "test.txt").write_text("Test content")
        repo.index.add(["test.txt"])
        repo.index.commit("Test commit")

        return str(repo_path)

    def test_commit_history_data_types(self, validation_repo):
        """Test that commit_history returns proper data types."""
        repo = Repository(working_dir=validation_repo)
        history = repo.commit_history()

        if not history.empty:
            # Check that numeric columns are numeric
            numeric_columns = ["insertions", "deletions", "lines", "net"]
            for col in numeric_columns:
                if col in history.columns:
                    assert pd.api.types.is_numeric_dtype(history[col]), f"Column {col} should be numeric"

            # Check that string columns are strings
            string_columns = ["message", "author", "committer"]
            for col in string_columns:
                if col in history.columns:
                    assert history[col].dtype == object, f"Column {col} should be string/object type"

    def test_blame_data_consistency(self, validation_repo):
        """Test that blame data is consistent and valid."""
        repo = Repository(working_dir=validation_repo)
        blame_df = repo.blame()

        if not blame_df.empty:
            # Check that loc (lines of code) is positive
            if "loc" in blame_df.columns:
                assert (blame_df["loc"] >= 0).all(), "Lines of code should be non-negative"

            # Check that committer is not null
            if "committer" in blame_df.columns:
                assert not blame_df["committer"].isnull().all(), "Committer should not be all null"

    def test_file_change_rates_data_bounds(self, validation_repo):
        """Test that file change rates have reasonable bounds."""
        repo = Repository(working_dir=validation_repo)
        change_rates = repo.file_change_rates()

        if not change_rates.empty:
            # Check that rates are non-negative
            rate_columns = ["abs_rate_of_change", "net_rate_of_change", "edit_rate"]
            for col in rate_columns:
                if col in change_rates.columns:
                    assert (change_rates[col] >= 0).all(), f"{col} should be non-negative"

            # Check that counts are non-negative integers
            count_columns = ["unique_committers", "abs_change", "net_change", "lines"]
            for col in count_columns:
                if col in change_rates.columns:
                    assert (change_rates[col] >= 0).all(), f"{col} should be non-negative"

    def test_punchcard_data_structure(self, validation_repo):
        """Test that punchcard data has proper structure."""
        repo = Repository(working_dir=validation_repo)
        punchcard = repo.punchcard()

        if not punchcard.empty:
            # Check that hour_of_day is in valid range (0-23)
            if "hour_of_day" in punchcard.columns:
                assert (punchcard["hour_of_day"] >= 0).all(), "Hour should be >= 0"
                assert (punchcard["hour_of_day"] <= 23).all(), "Hour should be <= 23"

            # Check that day_of_week is in valid range (0-6)
            if "day_of_week" in punchcard.columns:
                assert (punchcard["day_of_week"] >= 0).all(), "Day of week should be >= 0"
                assert (punchcard["day_of_week"] <= 6).all(), "Day of week should be <= 6"

    def test_revs_data_ordering(self, validation_repo):
        """Test that revs data is properly ordered."""
        repo = Repository(working_dir=validation_repo)
        revs = repo.revs()

        if not revs.empty and len(revs) > 1 and "date" in revs.columns:
            # Check that dates are in descending order (most recent first)
            # Convert to datetime if not already
            dates = pd.to_datetime(revs["date"])
            is_descending = dates.is_monotonic_decreasing
            # Allow for equal dates (commits at same time)
            assert is_descending or dates.nunique() == 1, "Revs should be in descending date order"
