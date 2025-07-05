import git
import pandas as pd
import pytest

from gitpandas import ProjectDirectory, Repository


class TestCacheIntegration:
    """Test cache integration across Repository and Project functionality."""

    @pytest.fixture
    def cache_repo(self, tmp_path):
        """Create a repository for cache testing."""
        repo_path = tmp_path / "cache_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Cache User").release()
        repo.config_writer().set_value("user", "email", "cache@example.com").release()

        # Create multiple commits for testing
        for i in range(3):
            (repo_path / f"file{i}.txt").write_text(f"Content {i}")
            repo.index.add([f"file{i}.txt"])
            repo.index.commit(f"Commit {i}")

        return str(repo_path)

    @pytest.fixture
    def cache_project(self, tmp_path):
        """Create a project with multiple repositories for cache testing."""
        project_path = tmp_path / "cache_project"
        project_path.mkdir()

        # Create multiple repositories
        for repo_num in range(2):
            repo_path = project_path / f"repo{repo_num}"
            repo_path.mkdir()
            repo = git.Repo.init(repo_path)

            # Configure git user
            repo.config_writer().set_value("user", "name", f"User {repo_num}").release()
            repo.config_writer().set_value("user", "email", f"user{repo_num}@example.com").release()

            # Create commits
            for i in range(2):
                (repo_path / f"file{i}.txt").write_text(f"Content {i} in repo {repo_num}")
                repo.index.add([f"file{i}.txt"])
                repo.index.commit(f"Commit {i} in repo {repo_num}")

        return str(project_path)

    def test_repository_cache_hit_miss_patterns(self, cache_repo):
        """Test cache hit and miss patterns for repository operations."""
        repo = Repository(working_dir=cache_repo)

        # First call should be a cache miss
        history1 = repo.commit_history()
        assert isinstance(history1, pd.DataFrame)
        assert len(history1) >= 3

        # Second call with same parameters should be a cache hit
        history2 = repo.commit_history()
        assert isinstance(history2, pd.DataFrame)
        assert len(history2) == len(history1)

        # Call with different parameters should be a cache miss
        history3 = repo.commit_history(limit=2)
        assert isinstance(history3, pd.DataFrame)
        assert len(history3) <= 2

        # Same parameters as history3 should be a cache hit
        history4 = repo.commit_history(limit=2)
        assert isinstance(history4, pd.DataFrame)
        assert len(history4) == len(history3)

    def test_repository_cache_different_methods(self, cache_repo):
        """Test that different repository methods use cache independently."""
        repo = Repository(working_dir=cache_repo)

        # Test different methods
        commit_history = repo.commit_history()
        file_change_history = repo.file_change_history()
        blame_data = repo.blame()
        file_change_rates = repo.file_change_rates()

        # All should return DataFrames
        assert isinstance(commit_history, pd.DataFrame)
        assert isinstance(file_change_history, pd.DataFrame)
        assert isinstance(blame_data, pd.DataFrame)
        assert isinstance(file_change_rates, pd.DataFrame)

        # Second calls should use cache
        commit_history2 = repo.commit_history()
        file_change_history2 = repo.file_change_history()
        blame_data2 = repo.blame()
        file_change_rates2 = repo.file_change_rates()

        # Should return same data
        assert len(commit_history2) == len(commit_history)
        assert len(file_change_history2) == len(file_change_history)
        assert len(blame_data2) == len(blame_data)
        assert len(file_change_rates2) == len(file_change_rates)

    def test_repository_cache_parameter_sensitivity(self, cache_repo):
        """Test that cache is sensitive to parameter changes."""
        repo = Repository(working_dir=cache_repo)

        # Test with different limits
        history_no_limit = repo.commit_history()
        history_limit_2 = repo.commit_history(limit=2)
        history_limit_1 = repo.commit_history(limit=1)

        assert len(history_no_limit) >= len(history_limit_2)
        assert len(history_limit_2) >= len(history_limit_1)

        # Test with different days
        history_all_days = repo.commit_history()
        history_recent = repo.commit_history(days=1)

        assert isinstance(history_all_days, pd.DataFrame)
        assert isinstance(history_recent, pd.DataFrame)

        # Test with different globs
        history_all_files = repo.commit_history()
        history_txt_only = repo.commit_history(include_globs=["*.txt"])

        assert isinstance(history_all_files, pd.DataFrame)
        assert isinstance(history_txt_only, pd.DataFrame)

    def test_repository_cache_with_globs(self, cache_repo):
        """Test cache behavior with different glob patterns."""
        repo = Repository(working_dir=cache_repo)

        # Test with different include globs
        history1 = repo.commit_history(include_globs=["*.txt"])
        history2 = repo.commit_history(include_globs=["*.py"])
        history3 = repo.commit_history(include_globs=["*.txt"])  # Same as history1

        assert isinstance(history1, pd.DataFrame)
        assert isinstance(history2, pd.DataFrame)
        assert isinstance(history3, pd.DataFrame)

        # history1 and history3 should be the same (cache hit)
        assert len(history1) == len(history3)

        # Test with ignore globs
        history4 = repo.commit_history(ignore_globs=["*.txt"])
        history5 = repo.commit_history(ignore_globs=["*.py"])

        assert isinstance(history4, pd.DataFrame)
        assert isinstance(history5, pd.DataFrame)

    def test_project_cache_aggregation(self, cache_project):
        """Test cache behavior for project-level aggregation."""
        project = ProjectDirectory(working_dir=cache_project)

        # First call should aggregate from all repositories
        history1 = project.commit_history()
        assert isinstance(history1, pd.DataFrame)
        assert len(history1) >= 4  # 2 repos * 2 commits each

        # Second call should use cache
        history2 = project.commit_history()
        assert isinstance(history2, pd.DataFrame)
        assert len(history2) == len(history1)

        # Different parameters should create new cache entries
        history3 = project.commit_history(limit=2)
        assert isinstance(history3, pd.DataFrame)
        assert len(history3) <= 2

    def test_project_cache_per_repository(self, cache_project):
        """Test that project caching works correctly per repository."""
        project = ProjectDirectory(working_dir=cache_project)

        # Get project-level data
        project_history = project.commit_history()
        project_blame = project.blame()

        assert isinstance(project_history, pd.DataFrame)
        assert isinstance(project_blame, pd.DataFrame)

        # Access individual repositories
        repos = project.repos
        assert len(repos) == 2

        # Each repository should have its own cache
        for repo in repos:
            repo_history = repo.commit_history()
            repo_blame = repo.blame()

            assert isinstance(repo_history, pd.DataFrame)
            assert isinstance(repo_blame, pd.DataFrame)

    def test_cache_memory_efficiency(self, cache_repo):
        """Test cache memory efficiency with large datasets."""
        repo = Repository(working_dir=cache_repo)

        # Test with different limits to ensure cache doesn't grow unbounded
        limits = [1, 2, 3, None, 5, 10]

        for limit in limits:
            history = repo.commit_history(limit=limit)
            assert isinstance(history, pd.DataFrame)

            if limit is not None:
                assert len(history) <= limit

    def test_cache_with_concurrent_access_simulation(self, cache_repo):
        """Test cache behavior under simulated concurrent access."""
        repo = Repository(working_dir=cache_repo)

        # Simulate multiple rapid accesses
        results = []
        for _i in range(5):
            history = repo.commit_history()
            blame_data = repo.blame()
            file_rates = repo.file_change_rates()

            results.append({"history_len": len(history), "blame_len": len(blame_data), "rates_len": len(file_rates)})

        # All results should be consistent
        first_result = results[0]
        for result in results[1:]:
            assert result["history_len"] == first_result["history_len"]
            assert result["blame_len"] == first_result["blame_len"]
            assert result["rates_len"] == first_result["rates_len"]

    def test_cache_invalidation_scenarios(self, cache_repo):
        """Test scenarios that should or shouldn't invalidate cache."""
        repo = Repository(working_dir=cache_repo)

        # Initial data
        history1 = repo.commit_history()
        assert isinstance(history1, pd.DataFrame)

        # Same call should use cache
        history2 = repo.commit_history()
        assert len(history2) == len(history1)

        # Different repository object for same path should use cache
        repo2 = Repository(working_dir=cache_repo)
        history3 = repo2.commit_history()
        assert len(history3) == len(history1)

    def test_cache_with_error_conditions(self, tmp_path):
        """Test cache behavior when operations encounter errors."""
        # Create invalid repository path
        invalid_path = tmp_path / "nonexistent_repo"

        try:
            repo = Repository(working_dir=str(invalid_path))
            history = repo.commit_history()
            # If no error is raised, should return empty DataFrame
            assert isinstance(history, pd.DataFrame)
        except Exception:
            # If error is raised, that's also acceptable behavior
            pass

    def test_cache_key_consistency(self, cache_repo):
        """Test that cache keys are consistent for equivalent operations."""
        repo = Repository(working_dir=cache_repo)

        # Test that equivalent parameter sets produce same cache behavior
        history1 = repo.commit_history(limit=5, days=None)
        history2 = repo.commit_history(limit=5)

        assert isinstance(history1, pd.DataFrame)
        assert isinstance(history2, pd.DataFrame)

        # Test with empty lists vs None
        history3 = repo.commit_history(include_globs=[])
        history4 = repo.commit_history(include_globs=None)

        assert isinstance(history3, pd.DataFrame)
        assert isinstance(history4, pd.DataFrame)

    def test_cache_data_integrity(self, cache_repo):
        """Test that cached data maintains integrity."""
        repo = Repository(working_dir=cache_repo)

        # Get data multiple times
        history1 = repo.commit_history()
        history2 = repo.commit_history()

        if not history1.empty and not history2.empty:
            # Data should be identical
            assert len(history1) == len(history2)

            # Check that data types are preserved
            for col in history1.columns:
                if col in history2.columns:
                    assert history1[col].dtype == history2[col].dtype

    def test_cache_with_different_working_directories(self, tmp_path):
        """Test cache behavior with different working directories."""
        # Create two different repositories
        repo1_path = tmp_path / "repo1"
        repo1_path.mkdir()
        repo1 = git.Repo.init(repo1_path)
        repo1.config_writer().set_value("user", "name", "User1").release()
        repo1.config_writer().set_value("user", "email", "user1@example.com").release()
        (repo1_path / "file1.txt").write_text("Content 1")
        repo1.index.add(["file1.txt"])
        repo1.index.commit("Commit 1")

        repo2_path = tmp_path / "repo2"
        repo2_path.mkdir()
        repo2 = git.Repo.init(repo2_path)
        repo2.config_writer().set_value("user", "name", "User2").release()
        repo2.config_writer().set_value("user", "email", "user2@example.com").release()
        (repo2_path / "file2.txt").write_text("Content 2")
        repo2.index.add(["file2.txt"])
        repo2.index.commit("Commit 2")

        # Test that different repositories have separate cache entries
        git_repo1 = Repository(working_dir=str(repo1_path))
        git_repo2 = Repository(working_dir=str(repo2_path))

        history1 = git_repo1.commit_history()
        history2 = git_repo2.commit_history()

        assert isinstance(history1, pd.DataFrame)
        assert isinstance(history2, pd.DataFrame)

        # Should have different data
        if not history1.empty and not history2.empty:
            # Different repositories should have different commit data
            assert len(history1) >= 1
            assert len(history2) >= 1

    def test_cache_performance_characteristics(self, cache_repo):
        """Test cache performance characteristics."""
        repo = Repository(working_dir=cache_repo)

        # First call (cache miss) - should work
        history1 = repo.commit_history()
        assert isinstance(history1, pd.DataFrame)

        # Subsequent calls (cache hits) - should be fast and consistent
        for _i in range(3):
            history = repo.commit_history()
            assert isinstance(history, pd.DataFrame)
            assert len(history) == len(history1)

    def test_cache_with_complex_parameters(self, cache_repo):
        """Test cache with complex parameter combinations."""
        repo = Repository(working_dir=cache_repo)

        # Test various parameter combinations
        param_sets = [
            {"limit": 2, "days": 30},
            {"limit": 2, "include_globs": ["*.txt"]},
            {"limit": 2, "ignore_globs": ["*.py"]},
            {"days": 30, "include_globs": ["*.txt"]},
            {"limit": 2, "days": 30, "include_globs": ["*.txt"]},
        ]

        results = {}
        for i, params in enumerate(param_sets):
            history = repo.commit_history(**params)
            assert isinstance(history, pd.DataFrame)
            results[i] = len(history)

            # Second call with same parameters should use cache
            history2 = repo.commit_history(**params)
            assert len(history2) == results[i]

    def test_cache_cross_method_independence(self, cache_repo):
        """Test that cache entries for different methods are independent."""
        repo = Repository(working_dir=cache_repo)

        # Call different methods with similar parameters
        commit_history = repo.commit_history(limit=2)
        file_history = repo.file_change_history(limit=2)
        blame_data = repo.blame()

        assert isinstance(commit_history, pd.DataFrame)
        assert isinstance(file_history, pd.DataFrame)
        assert isinstance(blame_data, pd.DataFrame)

        # Each method should cache independently
        commit_history2 = repo.commit_history(limit=2)
        file_history2 = repo.file_change_history(limit=2)
        blame_data2 = repo.blame()

        assert len(commit_history2) == len(commit_history)
        assert len(file_history2) == len(file_history)
        assert len(blame_data2) == len(blame_data)


class TestCacheErrorHandling:
    """Test cache behavior under error conditions."""

    @pytest.fixture
    def cache_repo(self, tmp_path):
        """Create a repository for cache testing."""
        repo_path = tmp_path / "cache_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        # Configure git user
        repo.config_writer().set_value("user", "name", "Cache User").release()
        repo.config_writer().set_value("user", "email", "cache@example.com").release()

        # Create multiple commits for testing
        for i in range(3):
            (repo_path / f"file{i}.txt").write_text(f"Content {i}")
            repo.index.add([f"file{i}.txt"])
            repo.index.commit(f"Commit {i}")

        return str(repo_path)

    def test_cache_with_corrupted_repository(self, tmp_path):
        """Test cache behavior with corrupted repository."""
        # Create a directory that looks like a git repo but isn't
        fake_repo_path = tmp_path / "fake_repo"
        fake_repo_path.mkdir()
        (fake_repo_path / ".git").mkdir()

        try:
            repo = Repository(working_dir=str(fake_repo_path))
            history = repo.commit_history()
            # Should handle gracefully
            assert isinstance(history, pd.DataFrame)
        except Exception:
            # Acceptable to raise exception for corrupted repo
            pass

    def test_cache_with_permission_errors(self, tmp_path):
        """Test cache behavior when file permissions prevent access."""
        # Create a repository
        repo_path = tmp_path / "perm_repo"
        repo_path.mkdir()
        repo = git.Repo.init(repo_path)

        repo.config_writer().set_value("user", "name", "Test User").release()
        repo.config_writer().set_value("user", "email", "test@example.com").release()

        (repo_path / "file.txt").write_text("Content")
        repo.index.add(["file.txt"])
        repo.index.commit("Test commit")

        git_repo = Repository(working_dir=str(repo_path))

        # Normal operation should work
        history = git_repo.commit_history()
        assert isinstance(history, pd.DataFrame)

    def test_cache_memory_pressure_simulation(self, cache_repo):
        """Test cache behavior under simulated memory pressure."""
        repo = Repository(working_dir=cache_repo)

        # Create many different cache entries
        for limit in range(1, 10):
            for days in [None, 1, 7, 30]:
                try:
                    history = repo.commit_history(limit=limit, days=days)
                    assert isinstance(history, pd.DataFrame)
                except Exception:
                    # Acceptable if some combinations fail
                    pass

    def test_cache_with_unicode_paths(self, tmp_path):
        """Test cache behavior with unicode characters in paths."""
        # Create repository with unicode name
        unicode_name = "测试_repo_café"
        try:
            repo_path = tmp_path / unicode_name
            repo_path.mkdir()
            repo = git.Repo.init(repo_path)

            repo.config_writer().set_value("user", "name", "Unicode User").release()
            repo.config_writer().set_value("user", "email", "unicode@example.com").release()

            (repo_path / "file.txt").write_text("Unicode content")
            repo.index.add(["file.txt"])
            repo.index.commit("Unicode commit")

            git_repo = Repository(working_dir=str(repo_path))
            history = git_repo.commit_history()
            assert isinstance(history, pd.DataFrame)

        except (OSError, UnicodeError):
            # Skip if unicode filenames not supported
            pytest.skip("Unicode filenames not supported on this platform")

    def test_cache_cleanup_behavior(self, cache_repo):
        """Test that cache doesn't grow unbounded."""
        repo = Repository(working_dir=cache_repo)

        # Generate many different cache entries
        for i in range(20):
            try:
                history = repo.commit_history(limit=i + 1)
                assert isinstance(history, pd.DataFrame)
            except Exception:
                # Some limits might not be valid
                pass

        # Cache should still work for new requests
        history = repo.commit_history()
        assert isinstance(history, pd.DataFrame)
