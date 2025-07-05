"""
Example demonstrating safe remote fetch and cache warming functionality.

This example shows how to use the new safe_fetch_remote and warm_cache methods
to keep repositories up to date and improve analysis performance through cache pre-warming.
"""

import logging
import os
import time

from definitions import GIT_PANDAS_DIR

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import DiskCache, EphemeralCache

# Configure logging to show git-pandas internal operations
# This demonstrates how users can monitor what git-pandas is doing internally
logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")

# Get the git-pandas logger to demonstrate internal operations
logger = logging.getLogger("gitpandas")


def demonstrate_repository_fetch_and_warm():
    """Demonstrate safe fetch and cache warming on a single repository.

    This function uses print() for demo output while git-pandas internal
    operations are logged via the configured logging system above.
    """
    print("Repository Safe Fetch and Cache Warming Demo")
    print("=" * 50)

    # Create a repository with caching enabled
    cache = EphemeralCache(max_keys=100)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    print(f"Repository: {repo.repo_name}")
    print(f"Cache backend: {type(cache).__name__}")
    print()

    # Demonstrate safe remote fetch
    print("1. Safe Remote Fetch")
    print("-" * 30)

    # First, try a dry run to see what would be fetched
    print("  Performing dry run fetch...")
    dry_run_result = repo.safe_fetch_remote(dry_run=True)
    print(f"  Dry run success: {dry_run_result['success']}")
    print(f"  Remote exists: {dry_run_result['remote_exists']}")
    print(f"  Message: {dry_run_result['message']}")
    print()

    # If we have remotes, try actual fetch
    if dry_run_result["remote_exists"]:
        print("  Performing actual fetch...")
        fetch_result = repo.safe_fetch_remote()
        print(f"  Fetch success: {fetch_result['success']}")
        print(f"  Changes available: {fetch_result['changes_available']}")
        print(f"  Message: {fetch_result['message']}")
        if fetch_result.get("error"):
            print(f"  Error: {fetch_result['error']}")
    else:
        print("  No remotes configured - skipping actual fetch")
    print()

    # Demonstrate cache warming
    print("2. Cache Warming")
    print("-" * 30)

    initial_cache_size = len(cache._cache)
    print(f"  Initial cache size: {initial_cache_size}")

    # Warm cache with default methods
    print("  Warming cache with default methods...")
    start_time = time.time()
    warm_result = repo.warm_cache()
    time.time()

    print(f"  Cache warming success: {warm_result['success']}")
    print(f"  Methods executed: {warm_result['methods_executed']}")
    print(f"  Methods failed: {warm_result['methods_failed']}")
    print(f"  Cache entries created: {warm_result['cache_entries_created']}")
    print(f"  Execution time: {warm_result['execution_time']:.2f} seconds")
    print(f"  Final cache size: {len(cache._cache)}")
    print()

    # Demonstrate custom cache warming
    print("3. Custom Cache Warming")
    print("-" * 30)

    print("  Warming cache with specific methods and parameters...")
    custom_warm_result = repo.warm_cache(
        methods=["commit_history", "branches", "file_detail"], limit=20, ignore_globs=["*.log", "*.tmp"]
    )

    print(f"  Custom warming success: {custom_warm_result['success']}")
    print(f"  Methods executed: {custom_warm_result['methods_executed']}")
    print(f"  Additional cache entries: {custom_warm_result['cache_entries_created']}")
    print()

    # Show performance improvement
    print("4. Performance Improvement Demo")
    print("-" * 30)

    # Clear cache and time cold operation
    cache._cache.clear()
    cache._key_list.clear()

    print("  Testing cold performance (no cache)...")
    start_time = time.time()
    repo.commit_history(limit=50)
    cold_time = time.time() - start_time
    print(f"  Cold operation time: {cold_time:.3f} seconds")

    # Now test warm performance
    print("  Testing warm performance (with cache)...")
    start_time = time.time()
    repo.commit_history(limit=50)
    warm_time = time.time() - start_time
    print(f"  Warm operation time: {warm_time:.3f} seconds")

    if cold_time > 0:
        speedup = cold_time / warm_time if warm_time > 0 else float("inf")
        print(f"  Performance improvement: {speedup:.1f}x faster")
    print()


def demonstrate_project_directory_bulk_operations():
    """Demonstrate bulk fetch and cache warming on multiple repositories."""
    print("ProjectDirectory Bulk Operations Demo")
    print("=" * 50)

    # For this demo, we'll use the current repository as our project directory
    # In practice, you would point this to a directory containing multiple repos
    cache = EphemeralCache(max_keys=200)

    # Create project directory with a single repository for demo
    project_dir = ProjectDirectory(working_dir=[GIT_PANDAS_DIR], cache_backend=cache)

    print(f"Project directory with {len(project_dir.repos)} repositories")
    print(f"Cache backend: {type(cache).__name__}")
    print()

    # Demonstrate bulk operations
    print("1. Bulk Fetch and Cache Warming")
    print("-" * 30)

    print("  Performing bulk operations...")
    bulk_result = project_dir.bulk_fetch_and_warm(
        fetch_remote=True,
        warm_cache=True,
        parallel=True,
        dry_run=False,
        cache_methods=["commit_history", "branches", "tags", "blame"],
        limit=100,
    )

    print(f"  Overall success: {bulk_result['success']}")
    print(f"  Repositories processed: {bulk_result['repositories_processed']}")
    print(f"  Execution time: {bulk_result['execution_time']:.2f} seconds")
    print()

    # Show fetch summary
    if bulk_result["fetch_results"]:
        print("  Fetch Summary:")
        print(f"    Successful: {bulk_result['summary']['fetch_successful']}")
        print(f"    Failed: {bulk_result['summary']['fetch_failed']}")
        print(f"    With remotes: {bulk_result['summary']['repositories_with_remotes']}")

    # Show cache summary
    if bulk_result["cache_results"]:
        print("  Cache Warming Summary:")
        print(f"    Successful: {bulk_result['summary']['cache_successful']}")
        print(f"    Failed: {bulk_result['summary']['cache_failed']}")
        print(f"    Total cache entries created: {bulk_result['summary']['total_cache_entries_created']}")
    print()

    # Show detailed results for each repository
    print("2. Detailed Results")
    print("-" * 30)

    for repo_name, fetch_result in bulk_result["fetch_results"].items():
        print(f"  Repository: {repo_name}")
        print(f"    Fetch success: {fetch_result['success']}")
        print(f"    Remote exists: {fetch_result['remote_exists']}")
        print(f"    Changes available: {fetch_result['changes_available']}")
        if fetch_result.get("error"):
            print(f"    Error: {fetch_result['error']}")

    for repo_name, cache_result in bulk_result["cache_results"].items():
        print(f"  Repository: {repo_name}")
        print(f"    Cache warming success: {cache_result['success']}")
        print(f"    Methods executed: {cache_result['methods_executed']}")
        print(f"    Cache entries created: {cache_result['cache_entries_created']}")
    print()


def demonstrate_persistent_cache_with_fetch():
    """Demonstrate using DiskCache with fetch and warm operations."""
    print("Persistent Cache with Fetch and Warm Demo")
    print("=" * 50)

    cache_file = "/tmp/gitpandas_fetch_warm_demo_cache.gz"

    # Clean up any existing cache file
    if os.path.exists(cache_file):
        os.remove(cache_file)

    print(f"Creating repository with DiskCache: {cache_file}")
    cache = DiskCache(filepath=cache_file, max_keys=100)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    # Perform fetch and warm operations
    print("  Performing safe fetch...")
    fetch_result = repo.safe_fetch_remote()
    print(f"  Fetch success: {fetch_result['success']}")

    print("  Warming cache...")
    warm_result = repo.warm_cache(methods=["commit_history", "branches"], limit=50)
    print(f"  Cache warming success: {warm_result['success']}")
    print(f"  Cache entries created: {warm_result['cache_entries_created']}")

    print(f"  Cache file size: {os.path.getsize(cache_file)} bytes")
    print()

    # Create a new repository instance from the same cache file
    print("  Creating new repository instance from saved cache...")
    cache2 = DiskCache(filepath=cache_file, max_keys=100)
    repo2 = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache2, default_branch="master")

    print(f"  Loaded cache contains {len(cache2._cache)} entries")

    # Test that cache is working
    print("  Testing cache performance...")
    start_time = time.time()
    repo2.commit_history(limit=50)
    cache_time = time.time() - start_time
    print(f"  Cached operation time: {cache_time:.3f} seconds")

    # Clean up
    if os.path.exists(cache_file):
        os.remove(cache_file)
        print(f"  Cleaned up cache file: {cache_file}")
    print()


def demonstrate_error_handling():
    """Demonstrate error handling in fetch and warm operations."""
    print("Error Handling Demo")
    print("=" * 30)

    cache = EphemeralCache(max_keys=50)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    # Test fetch with invalid remote name
    print("  Testing fetch with invalid remote name...")
    result = repo.safe_fetch_remote(remote_name="nonexistent")
    print(f"  Success: {result['success']}")
    print(f"  Message: {result['message']}")
    print()

    # Test cache warming with invalid methods
    print("  Testing cache warming with invalid methods...")
    result = repo.warm_cache(methods=["nonexistent_method", "branches"])
    print(f"  Success: {result['success']}")
    print(f"  Methods executed: {result['methods_executed']}")
    print(f"  Methods failed: {result['methods_failed']}")
    print(f"  Errors: {result['errors']}")
    print()


if __name__ == "__main__":
    try:
        demonstrate_repository_fetch_and_warm()
        print("\n" + "=" * 70 + "\n")

        demonstrate_project_directory_bulk_operations()
        print("\n" + "=" * 70 + "\n")

        demonstrate_persistent_cache_with_fetch()
        print("\n" + "=" * 70 + "\n")

        demonstrate_error_handling()

        print("\n" + "=" * 70)
        print("Summary:")
        print("- safe_fetch_remote() safely fetches from remote repositories")
        print("- warm_cache() pre-populates cache for better performance")
        print("- bulk_fetch_and_warm() handles multiple repositories efficiently")
        print("- All operations are safe and handle errors gracefully")
        print("- Cache warming can improve subsequent analysis performance significantly")

    except Exception as e:
        print(f"Error running demo: {e}")
        print("Make sure you're running this from the git-pandas directory")
