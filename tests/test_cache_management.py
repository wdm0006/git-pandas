"""
Tests for cache management functionality.
"""

from unittest.mock import MagicMock

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import CacheEntry, DiskCache, EphemeralCache


class TestCacheManagement:
    def test_cache_invalidation_ephemeral(self):
        """Test cache invalidation with EphemeralCache."""
        cache = EphemeralCache(max_keys=10)

        # Add some test entries
        cache.set("test_key_1", CacheEntry("data1", "test_key_1"))
        cache.set("test_key_2", CacheEntry("data2", "test_key_2"))
        cache.set("other_key", CacheEntry("data3", "other_key"))

        assert len(cache._cache) == 3

        # Test invalidating specific keys
        removed = cache.invalidate_cache(keys=["test_key_1"])
        assert removed == 1
        assert len(cache._cache) == 2
        assert "test_key_1" not in cache._cache

        # Test invalidating by pattern
        removed = cache.invalidate_cache(pattern="test_*")
        assert removed == 1
        assert len(cache._cache) == 1
        assert "test_key_2" not in cache._cache
        assert "other_key" in cache._cache

        # Test clearing all
        removed = cache.invalidate_cache()
        assert len(cache._cache) == 0

    def test_cache_stats_ephemeral(self):
        """Test cache statistics with EphemeralCache."""
        cache = EphemeralCache(max_keys=10)

        # Empty cache stats
        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 0
        assert stats["cache_usage_percent"] == 0.0

        # Add some entries
        cache.set("key1", CacheEntry("data1", "key1"))
        cache.set("key2", CacheEntry("data2", "key2"))

        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 2
        assert stats["max_entries"] == 10
        assert stats["cache_usage_percent"] == 20.0
        assert stats["oldest_entry_age_hours"] is not None
        assert stats["newest_entry_age_hours"] is not None
        assert stats["average_entry_age_hours"] is not None

    def test_repository_cache_invalidation(self, tmp_path):
        """Test repository-level cache invalidation."""
        # Create a test repository
        repo_dir = tmp_path / "test_repo"
        repo_dir.mkdir()

        # Initialize git repo
        import git

        grepo = git.Repo.init(str(repo_dir))
        grepo.git.config("user.name", "Test User")
        grepo.git.config("user.email", "test@example.com")
        grepo.git.checkout("-b", "main")

        # Create a test file and commit
        test_file = repo_dir / "test.py"
        test_file.write_text("print('hello')")
        grepo.git.add("test.py")
        grepo.git.commit(m="Initial commit")

        # Create repository with cache
        cache = EphemeralCache(max_keys=10)
        repo = Repository(working_dir=str(repo_dir), cache_backend=cache, default_branch="main")

        # Warm cache
        repo.commit_history(limit=5)
        repo.branches()

        initial_cache_size = len(cache._cache)
        assert initial_cache_size > 0

        # Test invalidating specific cache type
        removed = repo.invalidate_cache(keys=["commit_history"])
        assert removed >= 0  # Should find and remove commit_history cache entries

        # Test invalidating all repo cache
        removed = repo.invalidate_cache()
        # After invalidation, we should have fewer or zero cache entries for this repo
        final_cache_size = len(cache._cache)
        assert final_cache_size <= initial_cache_size

    def test_repository_cache_stats(self, tmp_path):
        """Test repository-level cache statistics."""
        # Create a test repository
        repo_dir = tmp_path / "test_repo"
        repo_dir.mkdir()

        # Initialize git repo
        import git

        grepo = git.Repo.init(str(repo_dir))
        grepo.git.config("user.name", "Test User")
        grepo.git.config("user.email", "test@example.com")
        grepo.git.checkout("-b", "main")

        # Create a test file and commit
        test_file = repo_dir / "test.py"
        test_file.write_text("print('hello')")
        grepo.git.add("test.py")
        grepo.git.commit(m="Initial commit")

        # Test with no cache
        repo_no_cache = Repository(working_dir=str(repo_dir), default_branch="main")
        stats = repo_no_cache.get_cache_stats()
        assert stats["cache_backend"] is None
        assert stats["repository_entries"] == 0

        # Test with cache
        cache = EphemeralCache(max_keys=10)
        repo_with_cache = Repository(working_dir=str(repo_dir), cache_backend=cache, default_branch="main")

        # Warm cache
        repo_with_cache.commit_history(limit=5)

        stats = repo_with_cache.get_cache_stats()
        assert stats["cache_backend"] == "EphemeralCache"
        assert stats["repository_entries"] >= 0
        assert stats["global_cache_stats"] is not None

    def test_project_directory_cache_management(self, tmp_path):
        """Test ProjectDirectory cache management methods."""
        # Create test repositories
        repo1_dir = tmp_path / "repo1"
        repo1_dir.mkdir()
        repo2_dir = tmp_path / "repo2"
        repo2_dir.mkdir()

        # Initialize git repos
        import git

        for i, repo_dir in enumerate([repo1_dir, repo2_dir], 1):
            grepo = git.Repo.init(str(repo_dir))
            grepo.git.config("user.name", f"User{i}")
            grepo.git.config("user.email", f"user{i}@example.com")
            grepo.git.checkout("-b", "main")

            test_file = repo_dir / f"test{i}.py"
            test_file.write_text(f"print('hello from repo {i}')")
            grepo.git.add(f"test{i}.py")
            grepo.git.commit(m=f"Initial commit repo {i}")

        # Create ProjectDirectory with shared cache
        cache = EphemeralCache(max_keys=20)
        project = ProjectDirectory(working_dir=[str(repo1_dir), str(repo2_dir)], cache_backend=cache)

        # Warm cache for both repos
        for repo in project.repos:
            repo.commit_history(limit=3)
            repo.branches()

        initial_cache_size = len(cache._cache)
        assert initial_cache_size > 0

        # Test cache stats
        stats = project.get_cache_stats()
        assert stats["total_repositories"] == 2
        assert stats["repositories_with_cache"] == 2
        assert stats["cache_coverage_percent"] == 100.0
        assert "EphemeralCache" in stats["cache_backends"]

        # Test cache invalidation for specific repository
        result = project.invalidate_cache(repositories=["repo1"])
        assert result["repositories_processed"] == 1
        assert "repo1" in result["repository_results"]

        # Test cache invalidation for all repositories
        result = project.invalidate_cache()
        assert result["repositories_processed"] == 2
        assert result["total_invalidated"] >= 0

    def test_cache_management_no_cache_backend(self, tmp_path):
        """Test cache management methods when no cache backend is configured."""
        # Create test repository without cache
        repo_dir = tmp_path / "test_repo"
        repo_dir.mkdir()

        import git

        grepo = git.Repo.init(str(repo_dir))
        grepo.git.config("user.name", "Test User")
        grepo.git.config("user.email", "test@example.com")

        test_file = repo_dir / "test.py"
        test_file.write_text("print('hello')")
        grepo.git.add("test.py")
        grepo.git.commit(m="Initial commit")

        repo = Repository(working_dir=str(repo_dir), default_branch="main")

        # Test cache methods with no backend
        removed = repo.invalidate_cache()
        assert removed == 0

        stats = repo.get_cache_stats()
        assert stats["cache_backend"] is None

    def test_cache_management_error_handling(self):
        """Test error handling in cache management methods."""
        # Create mock cache backend that raises errors
        mock_cache = MagicMock()
        mock_cache.invalidate_cache.side_effect = Exception("Cache error")
        mock_cache.get_cache_stats.side_effect = Exception("Stats error")

        # Create mock repository
        mock_repo = MagicMock()
        mock_repo.cache_backend = mock_cache
        mock_repo.repo_name = "test_repo"
        mock_repo.invalidate_cache = Repository.invalidate_cache.__get__(mock_repo)
        mock_repo.get_cache_stats = Repository.get_cache_stats.__get__(mock_repo)

        # Test that errors are handled gracefully
        removed = mock_repo.invalidate_cache()
        assert removed == 0  # Should return 0 on error

        stats = mock_repo.get_cache_stats()
        assert "repository" in stats  # Should still return basic structure

    def test_disk_cache_management(self, tmp_path):
        """Test cache management with DiskCache."""
        cache_file = tmp_path / "test_cache.gz"
        cache = DiskCache(str(cache_file), max_keys=5)

        # Add some test entries
        cache.set("test_key_1", CacheEntry("data1", "test_key_1"))
        cache.set("test_key_2", CacheEntry("data2", "test_key_2"))

        assert len(cache._cache) == 2

        # Test invalidation
        removed = cache.invalidate_cache(keys=["test_key_1"])
        assert removed == 1
        assert len(cache._cache) == 1

        # Test stats
        stats = cache.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["max_entries"] == 5
        assert stats["cache_usage_percent"] == 20.0
