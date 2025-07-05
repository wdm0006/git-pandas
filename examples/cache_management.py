"""
Example demonstrating cache management functionality.

This example shows how to use the new cache management methods to:
- Monitor cache statistics
- Invalidate specific cache entries
- Clear cache selectively or entirely
"""

import logging
import os
import time

from definitions import GIT_PANDAS_DIR

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import DiskCache, EphemeralCache

# Configure logging to show cache operations
logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gitpandas")


def demonstrate_repository_cache_management():
    """Demonstrate cache management for a single repository."""
    print("Repository Cache Management Demo")
    print("=" * 40)

    # Create repository with EphemeralCache
    cache = EphemeralCache(max_keys=50)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    print(f"Repository: {repo.repo_name}")
    print(f"Cache backend: {type(cache).__name__}")
    print()

    # 1. Initial cache stats
    print("1. Initial Cache Statistics")
    print("-" * 30)
    stats = repo.get_cache_stats()
    print(f"  Repository entries: {stats['repository_entries']}")
    print(f"  Cache backend: {stats['cache_backend']}")
    if stats["global_cache_stats"]:
        global_stats = stats["global_cache_stats"]
        print(f"  Total cache entries: {global_stats['total_entries']}")
        print(f"  Cache usage: {global_stats['cache_usage_percent']:.1f}%")
    print()

    # 2. Warm cache with various methods
    print("2. Warming Cache")
    print("-" * 30)
    print("  Executing commit_history...")
    repo.commit_history(limit=20)

    print("  Executing branches...")
    repo.branches()

    print("  Executing blame...")
    repo.blame(by="repository")

    print("  Executing bus_factor...")
    repo.bus_factor()

    # Check cache stats after warming
    stats = repo.get_cache_stats()
    global_stats = stats.get("global_cache_stats", {})
    print(f"  After warming - Repository entries: {stats['repository_entries']}")
    print(f"  After warming - Total entries: {global_stats.get('total_entries', 0)}")
    print(f"  After warming - Cache usage: {global_stats.get('cache_usage_percent', 0):.1f}%")
    print()

    # 3. Demonstrate selective cache invalidation
    print("3. Selective Cache Invalidation")
    print("-" * 30)

    # Invalidate commit_history related cache entries
    print("  Invalidating commit_history cache entries...")
    removed = repo.invalidate_cache(keys=["commit_history"])
    print(f"  Removed {removed} cache entries")

    # Check stats after selective invalidation
    stats = repo.get_cache_stats()
    global_stats = stats.get("global_cache_stats", {})
    print(f"  After selective invalidation - Repository entries: {stats['repository_entries']}")
    print(f"  After selective invalidation - Total entries: {global_stats.get('total_entries', 0)}")
    print()

    # 4. Demonstrate pattern-based invalidation
    print("4. Pattern-based Cache Invalidation")
    print("-" * 30)

    # Invalidate all blame-related entries
    print("  Invalidating blame* cache entries...")
    removed = repo.invalidate_cache(pattern="blame*")
    print(f"  Removed {removed} cache entries")
    print()

    # 5. Complete cache invalidation for repository
    print("5. Complete Repository Cache Invalidation")
    print("-" * 30)

    print("  Clearing all cache entries for this repository...")
    removed = repo.invalidate_cache()
    print(f"  Removed {removed} cache entries")

    # Final stats
    stats = repo.get_cache_stats()
    global_stats = stats.get("global_cache_stats", {})
    print(f"  Final - Repository entries: {stats['repository_entries']}")
    print(f"  Final - Total entries: {global_stats.get('total_entries', 0)}")
    print()


def demonstrate_project_cache_management():
    """Demonstrate cache management across multiple repositories."""
    print("Project Directory Cache Management Demo")
    print("=" * 45)

    # Create a project directory with shared cache
    cache = EphemeralCache(max_keys=100)

    # For this demo, we'll use the current repository as our project
    project = ProjectDirectory(working_dir=[GIT_PANDAS_DIR], cache_backend=cache)

    print(f"Project with {len(project.repos)} repositories")
    print(f"Shared cache backend: {type(cache).__name__}")
    print()

    # 1. Initial project cache stats
    print("1. Initial Project Cache Statistics")
    print("-" * 35)
    stats = project.get_cache_stats()
    print(f"  Total repositories: {stats['total_repositories']}")
    print(f"  Repositories with cache: {stats['repositories_with_cache']}")
    print(f"  Cache coverage: {stats['cache_coverage_percent']:.1f}%")
    print(f"  Total cache entries: {stats['total_cache_entries']}")
    if stats["cache_backends"]:
        for backend, count in stats["cache_backends"].items():
            print(f"  Cache backend {backend}: {count} repositories")
    print()

    # 2. Warm cache across repositories
    print("2. Warming Cache Across Repositories")
    print("-" * 35)
    for repo in project.repos:
        print(f"  Warming cache for {repo.repo_name}...")
        repo.commit_history(limit=15)
        repo.branches()
        repo.file_detail()

    # Check stats after warming
    stats = project.get_cache_stats()
    print(f"  After warming - Total cache entries: {stats['total_cache_entries']}")
    if stats["global_stats"]:
        print(f"  After warming - Cache usage: {stats['global_stats']['cache_usage_percent']:.1f}%")
    print()

    # 3. Repository-specific cache invalidation
    print("3. Repository-specific Cache Invalidation")
    print("-" * 35)

    if project.repos:
        target_repo = project.repos[0].repo_name
        print(f"  Invalidating cache for repository: {target_repo}")
        result = project.invalidate_cache(repositories=[target_repo])

        print(f"  Repositories processed: {result['repositories_processed']}")
        print(f"  Total invalidated: {result['total_invalidated']}")

        for repo_name, repo_result in result["repository_results"].items():
            if repo_result["success"]:
                print(f"    {repo_name}: {repo_result['invalidated']} entries removed")
            else:
                print(f"    {repo_name}: Error - {repo_result.get('error', 'Unknown')}")
    print()

    # 4. Pattern-based cache invalidation across project
    print("4. Pattern-based Cache Invalidation")
    print("-" * 35)

    print("  Invalidating commit_history* across all repositories...")
    result = project.invalidate_cache(pattern="commit_history*")
    print(f"  Total invalidated: {result['total_invalidated']}")
    print()

    # 5. Complete project cache clearing
    print("5. Complete Project Cache Clearing")
    print("-" * 35)

    print("  Clearing all cache entries for the project...")
    result = project.invalidate_cache()
    print(f"  Total invalidated: {result['total_invalidated']}")

    # Final stats
    stats = project.get_cache_stats()
    print(f"  Final - Total cache entries: {stats['total_cache_entries']}")
    print()


def demonstrate_persistent_cache_management():
    """Demonstrate cache management with persistent DiskCache."""
    print("Persistent Cache Management Demo")
    print("=" * 35)

    cache_file = "/tmp/gitpandas_cache_mgmt_demo.gz"

    # Clean up any existing cache file
    if os.path.exists(cache_file):
        os.remove(cache_file)

    print(f"Creating repository with DiskCache: {cache_file}")
    cache = DiskCache(filepath=cache_file, max_keys=30)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    # 1. Demonstrate cache persistence
    print("1. Building Persistent Cache")
    print("-" * 30)

    print("  Adding data to cache...")
    repo.commit_history(limit=10)
    repo.branches()
    repo.tags()

    stats = repo.get_cache_stats()
    global_stats = stats.get("global_cache_stats", {})
    initial_entries = global_stats.get("total_entries", 0)
    print(f"  Cache entries created: {initial_entries}")
    print(f"  Cache file size: {os.path.getsize(cache_file)} bytes")
    print()

    # 2. Demonstrate selective invalidation with persistence
    print("2. Selective Invalidation with Persistence")
    print("-" * 30)

    print("  Invalidating branches cache...")
    removed = repo.invalidate_cache(keys=["branches"])
    print(f"  Removed {removed} entries")

    stats = repo.get_cache_stats()
    global_stats = stats.get("global_cache_stats", {})
    remaining_entries = global_stats.get("total_entries", 0)
    print(f"  Remaining entries: {remaining_entries}")
    print(f"  Updated cache file size: {os.path.getsize(cache_file)} bytes")
    print()

    # 3. Demonstrate cache stats details
    print("3. Detailed Cache Statistics")
    print("-" * 30)

    if global_stats:
        print(f"  Total entries: {global_stats.get('total_entries', 0)}")
        print(f"  Max entries: {global_stats.get('max_entries', 0)}")
        print(f"  Cache usage: {global_stats.get('cache_usage_percent', 0):.1f}%")

        if global_stats.get("oldest_entry_age_hours") is not None:
            print(f"  Oldest entry age: {global_stats['oldest_entry_age_hours']:.2f} hours")
            print(f"  Newest entry age: {global_stats['newest_entry_age_hours']:.2f} hours")
            print(f"  Average entry age: {global_stats['average_entry_age_hours']:.2f} hours")
    print()

    # 4. Test cache reload from disk
    print("4. Cache Reload from Disk")
    print("-" * 30)

    print("  Creating new repository instance from saved cache...")
    cache2 = DiskCache(filepath=cache_file, max_keys=30)
    repo2 = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache2, default_branch="master")

    stats2 = repo2.get_cache_stats()
    global_stats2 = stats2.get("global_cache_stats", {})
    loaded_entries = global_stats2.get("total_entries", 0)
    print(f"  Loaded cache entries: {loaded_entries}")
    print("  Cache successfully reloaded from disk")
    print()

    # Clean up
    if os.path.exists(cache_file):
        os.remove(cache_file)
        print(f"  Cleaned up cache file: {cache_file}")
    print()


def demonstrate_cache_monitoring():
    """Demonstrate cache monitoring and analysis."""
    print("Cache Monitoring and Analysis Demo")
    print("=" * 35)

    cache = EphemeralCache(max_keys=20)
    repo = Repository(working_dir=GIT_PANDAS_DIR, cache_backend=cache, default_branch="master")

    print("1. Cache Performance Monitoring")
    print("-" * 30)

    # Measure cold vs warm performance
    print("  Testing cold performance (no cache)...")
    start_time = time.time()
    repo.commit_history(limit=25)
    cold_time = time.time() - start_time
    print(f"  Cold execution time: {cold_time:.3f} seconds")

    print("  Testing warm performance (with cache)...")
    start_time = time.time()
    repo.commit_history(limit=25)
    warm_time = time.time() - start_time
    print(f"  Warm execution time: {warm_time:.3f} seconds")

    if cold_time > 0 and warm_time > 0:
        speedup = cold_time / warm_time
        print(f"  Performance improvement: {speedup:.1f}x faster with cache")
    print()

    # 2. Cache aging analysis
    print("2. Cache Aging Analysis")
    print("-" * 30)

    # Add more cache entries with some time delay
    repo.branches()
    time.sleep(0.1)  # Small delay to show age differences
    repo.tags()

    stats = repo.get_cache_stats()
    global_stats = stats.get("global_cache_stats", {})

    if global_stats and global_stats.get("oldest_entry_age_hours") is not None:
        oldest = global_stats["oldest_entry_age_hours"] * 3600  # Convert to seconds
        newest = global_stats["newest_entry_age_hours"] * 3600
        average = global_stats["average_entry_age_hours"] * 3600

        print(f"  Oldest cache entry: {oldest:.2f} seconds old")
        print(f"  Newest cache entry: {newest:.2f} seconds old")
        print(f"  Average cache age: {average:.2f} seconds")

    # 3. Cache key inspection
    print("\n3. Cache Key Inspection")
    print("-" * 30)

    if hasattr(cache, "list_cached_keys"):
        cached_keys = cache.list_cached_keys()
        print(f"  Total cached keys: {len(cached_keys)}")

        if cached_keys:
            print("  Sample cache keys:")
            for i, key_info in enumerate(cached_keys[:3]):  # Show first 3 keys
                key = key_info.get("key", "N/A")
                age = key_info.get("age_seconds", 0)
                print(f"    {i + 1}. {key} (age: {age:.2f}s)")
    print()


if __name__ == "__main__":
    try:
        demonstrate_repository_cache_management()
        print("\n" + "=" * 70 + "\n")

        demonstrate_project_cache_management()
        print("\n" + "=" * 70 + "\n")

        demonstrate_persistent_cache_management()
        print("\n" + "=" * 70 + "\n")

        demonstrate_cache_monitoring()

        print("\n" + "=" * 70)
        print("Summary:")
        print("- invalidate_cache() removes specific or all cache entries")
        print("- get_cache_stats() provides detailed cache usage information")
        print("- Cache management works with all cache backends (Ephemeral, Disk, Redis)")
        print("- Repository and ProjectDirectory support cache management")
        print("- Cache persistence allows for cross-session cache management")

    except Exception as e:
        print(f"Error running demo: {e}")
        print("Make sure you're running this from the git-pandas directory")
