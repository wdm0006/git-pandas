"""
Example of accessing cache timestamp information.

This example demonstrates how users can check when cache entries were populated
without any changes to the Repository or ProjectDirectory API.
"""

import os
import time
from datetime import datetime, timezone

from definitions import GIT_PANDAS_DIR

from gitpandas import Repository
from gitpandas.cache import DiskCache, EphemeralCache


def demonstrate_cache_timestamps():
    """Demonstrate accessing cache timestamp information."""
    print("Cache Timestamp Information Demo")
    print("=" * 40)

    # Create a repository with a cache backend
    cache = EphemeralCache(max_keys=100)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    print(f"Repository: {repo.repo_name}")
    print(f"Cache backend: {type(cache).__name__}")
    print()

    # Call some methods to populate the cache
    print("Populating cache with repository data...")

    print("  - Getting commit history...")
    repo.commit_history(limit=10)

    print("  - Getting file list...")
    repo.list_files()

    print("  - Getting blame information...")
    repo.blame()

    print(f"Cache now contains {len(cache._cache)} entries")
    print()

    # Show cache information
    print("Cache Contents and Timestamps:")
    print("-" * 40)

    cached_keys = cache.list_cached_keys()
    for entry in cached_keys:
        print(f"Key: {entry['key']}")
        print(f"  Cached at: {entry['cached_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Age: {entry['age_seconds']:.1f} seconds")
        print()

    # Wait a moment and call one method again
    print("Waiting 2 seconds and refreshing commit history...")
    time.sleep(2)

    # This should hit the cache
    repo.commit_history(limit=10)

    # This should create a new cache entry
    repo.commit_history(limit=20)

    print("\nUpdated cache contents:")
    print("-" * 40)

    cached_keys = cache.list_cached_keys()
    for entry in cached_keys:
        print(f"Key: {entry['key']}")
        print(f"  Cached at: {entry['cached_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Age: {entry['age_seconds']:.1f} seconds")
        print()

    # Demonstrate getting specific cache info
    print("Getting specific cache information:")
    print("-" * 40)

    # Find a commit_history cache key
    commit_keys = [k for k in cached_keys if "commit_history" in k["key"]]
    if commit_keys:
        key = commit_keys[0]["key"]
        info = cache.get_cache_info(key)
        if info:
            print(f"Cache info for key '{key}':")
            print(f"  Cached at: {info['cached_at']}")
            print(f"  Age: {info['age_minutes']:.2f} minutes")
            print(f"  Age: {info['age_hours']:.4f} hours")


def demonstrate_disk_cache_persistence():
    """Demonstrate cache persistence with DiskCache."""
    print("\n" + "=" * 50)
    print("Disk Cache Persistence Demo")
    print("=" * 50)

    cache_file = "/tmp/gitpandas_demo_cache.gz"

    # Clean up any existing cache file
    if os.path.exists(cache_file):
        os.remove(cache_file)

    print("Creating repository with DiskCache...")
    cache = DiskCache(filepath=cache_file, max_keys=50)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    # Populate cache
    print("Populating cache...")
    repo.commit_history(limit=5)
    repo.list_files()

    print(f"Cache file created: {cache_file}")
    print(f"Cache contains {len(cache._cache)} entries")

    # Show initial cache info
    cached_keys = cache.list_cached_keys()
    print("\nInitial cache entries:")
    for entry in cached_keys:
        print(f"  {entry['key']}: {entry['cached_at'].strftime('%H:%M:%S')}")

    # Create a new cache instance from the same file
    print("\nCreating new cache instance from saved file...")
    cache2 = DiskCache(filepath=cache_file, max_keys=50)

    print(f"Loaded cache contains {len(cache2._cache)} entries")

    # Show loaded cache info
    cached_keys2 = cache2.list_cached_keys()
    print("\nLoaded cache entries:")
    for entry in cached_keys2:
        print(f"  {entry['key']}: {entry['cached_at'].strftime('%H:%M:%S')} (age: {entry['age_seconds']:.1f}s)")

    # Clean up
    if os.path.exists(cache_file):
        os.remove(cache_file)
        print(f"\nCleaned up cache file: {cache_file}")


def demonstrate_cache_with_force_refresh():
    """Demonstrate cache behavior with force_refresh."""
    print("\n" + "=" * 50)
    print("Force Refresh Demo")
    print("=" * 50)

    cache = EphemeralCache(max_keys=10)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    print("Getting commit history (first time)...")
    start_time = datetime.now(timezone.utc)
    repo.commit_history(limit=5)

    time.sleep(1)

    print("Getting commit history (should use cache)...")
    repo.commit_history(limit=5)

    time.sleep(1)

    print("Getting commit history with force_refresh=True...")
    repo.commit_history(limit=5, force_refresh=True)

    print("\nCache timeline:")
    cached_keys = cache.list_cached_keys()
    for entry in cached_keys:
        if "commit_history" in entry["key"]:
            age_from_start = (entry["cached_at"] - start_time).total_seconds()
            print(f"  Commit history cached at: +{age_from_start:.1f}s from start")
            print(f"  Current age: {entry['age_seconds']:.1f}s")


if __name__ == "__main__":
    try:
        demonstrate_cache_timestamps()
        demonstrate_disk_cache_persistence()
        demonstrate_cache_with_force_refresh()

        print("\n" + "=" * 50)
        print("Summary:")
        print("- Cache backends now track when entries were created")
        print("- No changes to Repository or ProjectDirectory API")
        print("- Users can access cache info via cache_backend.get_cache_info()")
        print("- Users can list all cached keys via cache_backend.list_cached_keys()")
        print("- Backward compatibility maintained with existing caches")

    except Exception as e:
        print(f"Error running demo: {e}")
        print("Make sure you're running this from the git-pandas directory")
