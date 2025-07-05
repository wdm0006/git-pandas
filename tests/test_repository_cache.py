import os
import shutil
import tempfile
from unittest import mock

import pytest
from git import Repo

from gitpandas import Repository
from gitpandas.cache import DiskCache


@pytest.fixture
def temp_git_repo():
    """Create a temporary git repository for testing."""
    temp_dir = tempfile.mkdtemp()

    # Initialize git repo
    repo = Repo.init(temp_dir)
    # Explicitly create and checkout 'main' branch
    repo.git.checkout(b="main")

    # Create a test file
    test_file_path = os.path.join(temp_dir, "test_file.txt")
    with open(test_file_path, "w") as f:
        f.write("Initial content")

    # Add and commit
    repo.git.add(test_file_path)
    repo.git.commit("-m", "Initial commit")

    # Create a second file and commit
    test_file2_path = os.path.join(temp_dir, "test_file2.txt")
    with open(test_file2_path, "w") as f:
        f.write("Second file content")

    # Add and commit
    repo.git.add(test_file2_path)
    repo.git.commit("-m", "Second commit")

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def temp_cache_file():
    """Create a temporary cache file."""
    fd, path = tempfile.mkstemp(suffix=".gz")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


class TestRepositoryCache:
    """Test caching behavior with the actual Repository class."""

    def test_repository_list_files_cache(self, temp_git_repo, temp_cache_file):
        """Test that list_files method properly uses cache."""
        # Create cache and repository
        cache = DiskCache(filepath=temp_cache_file)

        # Mock the cache _get_entry and set methods to track calls
        with (
            mock.patch.object(cache, "_get_entry", wraps=cache._get_entry) as mock_get_entry,
            mock.patch.object(cache, "set", wraps=cache.set) as mock_set,
        ):
            repo = Repository(working_dir=temp_git_repo, cache_backend=cache)

            # First call - should set cache but not get from it
            result1 = repo.list_files()
            assert mock_set.call_count > 0, "Cache set should be called"
            assert mock_get_entry.call_count > 0, "Cache _get_entry should be called (but returns miss)"
            mock_set.reset_mock()
            mock_get_entry.reset_mock()

            # Second call - should get from cache
            result2 = repo.list_files()
            assert mock_get_entry.call_count > 0, "Cache _get_entry should be called"
            assert mock_set.call_count == 0, "Cache set should not be called"

            # Results should be identical
            assert result1.equals(result2), "Results should be identical when using cache"

            # Force refresh - should set cache again
            mock_set.reset_mock()
            mock_get_entry.reset_mock()
            result3 = repo.list_files(force_refresh=True)
            assert mock_set.call_count > 0, "Cache set should be called with force_refresh"

            # Results should match (unchanged repo)
            assert result1.equals(result3), "Results should match even with force_refresh"

    def test_repository_cache_with_different_params(self, temp_git_repo, temp_cache_file):
        """Test caching with different parameters."""
        cache = DiskCache(filepath=temp_cache_file)

        # Spy on the cache's set method
        with mock.patch.object(cache, "set", wraps=cache.set) as mock_set:
            repo = Repository(working_dir=temp_git_repo, cache_backend=cache)

            # Call with default revision (HEAD)
            repo.list_files()
            first_call_count = mock_set.call_count
            assert first_call_count > 0, "Cache set should be called"
            mock_set.reset_mock()

            # Call with same parameters - should use cache
            repo.list_files()
            assert mock_set.call_count == 0, "Cache set should not be called for same parameters"

            # Call with different revision - should set new cache entry
            repo.list_files(rev="HEAD~1")
            assert mock_set.call_count > 0, "Cache set should be called for different parameters"

    def test_repository_cache_persistence(self, temp_git_repo, temp_cache_file):
        """Test that cache persists between Repository instances."""
        # First repository
        cache1 = DiskCache(filepath=temp_cache_file)
        repo1 = Repository(working_dir=temp_git_repo, cache_backend=cache1)

        # Call method and get result
        result1 = repo1.list_files()

        # Create second repository with same cache file
        cache2 = DiskCache(filepath=temp_cache_file)

        # Mock the _get_entry method to verify it's called
        with (
            mock.patch.object(cache2, "_get_entry", wraps=cache2._get_entry) as mock_get_entry,
            mock.patch.object(cache2, "set", wraps=cache2.set) as mock_set,
        ):
            repo2 = Repository(working_dir=temp_git_repo, cache_backend=cache2)

            # Call same method - should use cache
            result2 = repo2.list_files()
            assert mock_get_entry.call_count > 0, "Cache _get_entry should be called"
            assert mock_set.call_count == 0, "Cache set should not be called"

            # Results should match
            assert result1.equals(result2), "Results should match between repository instances"

    def test_multiple_repository_methods_cache(self, temp_git_repo, temp_cache_file):
        """Test caching behavior across different repository methods."""
        cache = DiskCache(filepath=temp_cache_file)
        repo = Repository(working_dir=temp_git_repo, cache_backend=cache)

        # Create a dictionary to store original results
        results = {}

        # First calls - should create cache entries
        results["commits"] = repo.commit_history()
        results["files"] = repo.list_files()

        # Mock the cache _get_entry method to track calls
        with mock.patch.object(cache, "_get_entry", wraps=cache._get_entry) as mock_get_entry:
            # Second calls - should use cache
            commits2 = repo.commit_history()
            files2 = repo.list_files()

            # Both should match original results
            assert commits2.equals(results["commits"]), "Commit results should match"
            assert files2.equals(results["files"]), "File results should match"

            # Verify _get_entry was called for both
            assert mock_get_entry.call_count >= 2, "Cache _get_entry should be called for both methods"

            # Force refresh one method
            mock_get_entry.reset_mock()
            with mock.patch.object(cache, "set", wraps=cache.set) as mock_set:
                commits3 = repo.commit_history(force_refresh=True)
                assert commits3.equals(results["commits"]), "Results should still match after force refresh"
                assert mock_set.call_count > 0, "Cache set should be called for force refresh"

                # Other method should still use cache
                mock_get_entry.reset_mock()
                files3 = repo.list_files()
                assert files3.equals(results["files"]), "Files should still match"
                assert mock_get_entry.call_count > 0, "Cache _get_entry should be called for unchanged method"
