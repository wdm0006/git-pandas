Performance Guide
=================

This guide covers performance optimization strategies and best practices for using git-pandas effectively with large repositories and datasets.

Overview
--------

Git-pandas provides several mechanisms to optimize performance:

- **Caching**: Multiple cache backends for reusing expensive computations
- **Parallelization**: Multi-threading support for repository operations
- **Data Filtering**: Glob patterns and limits to reduce dataset size
- **Memory Management**: Efficient data structures and memory usage patterns

Caching System
--------------

The caching system is the most important performance optimization in git-pandas. It stores the results of expensive Git operations and analysis computations.

Cache Backends
~~~~~~~~~~~~~~

git-pandas supports three cache backends:

**EphemeralCache** (In-Memory)
  - Best for: Single-session analysis, development, testing
  - Pros: Fast access, no disk I/O, automatic cleanup
  - Cons: Data lost when process ends, limited by available RAM
  - Use case: Interactive analysis, Jupyter notebooks

**DiskCache** (Persistent)
  - Best for: Multi-session analysis, CI/CD pipelines, long-running processes
  - Pros: Survives process restarts, configurable size limits, compression
  - Cons: Slower than memory cache, disk space usage
  - Use case: Regular analysis workflows, automated reporting

**RedisDFCache** (Distributed)
  - Best for: Multi-user environments, distributed analysis, shared cache
  - Pros: Shared across processes/machines, TTL support, Redis features
  - Cons: Requires Redis server, network latency, additional complexity
  - Use case: Team environments, production deployments

Cache Configuration
~~~~~~~~~~~~~~~~~~~

Basic cache setup:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import EphemeralCache, DiskCache, RedisDFCache

    # In-memory cache (fastest for single session)
    cache = EphemeralCache(max_keys=1000)
    repo = Repository('/path/to/repo', cache_backend=cache)

    # Persistent cache (best for repeated analysis)
    cache = DiskCache('/tmp/gitpandas_cache.gz', max_keys=500)
    repo = Repository('/path/to/repo', cache_backend=cache)

    # Redis cache (best for shared environments)
    cache = RedisDFCache(host='localhost', max_keys=2000, ttl=3600)
    repo = Repository('/path/to/repo', cache_backend=cache)

Cache Warming
~~~~~~~~~~~~~

Pre-populate cache for better performance:

.. code-block:: python

    # Warm cache with commonly used methods
    result = repo.warm_cache(
        methods=['commit_history', 'branches', 'blame', 'file_detail'],
        limit=100,  # Reasonable limit for cache warming
        ignore_globs=['*.log', '*.tmp']
    )
    
    print(f"Cache entries created: {result['cache_entries_created']}")
    print(f"Execution time: {result['execution_time']:.2f} seconds")

Cache Management
~~~~~~~~~~~~~~~~

Monitor and manage cache performance:

.. code-block:: python

    # Get cache statistics
    stats = repo.get_cache_stats()
    print(f"Repository entries: {stats['repository_entries']}")
    if stats['global_cache_stats']:
        global_stats = stats['global_cache_stats']
        print(f"Cache usage: {global_stats['cache_usage_percent']:.1f}%")
        print(f"Average entry age: {global_stats['average_entry_age_hours']:.2f} hours")

    # Invalidate specific cache entries
    repo.invalidate_cache(keys=['commit_history'])
    
    # Clear old cache entries by pattern
    repo.invalidate_cache(pattern='blame*')
    
    # Clear all cache for repository
    repo.invalidate_cache()

Performance Benchmarks
~~~~~~~~~~~~~~~~~~~~~~~

Typical performance improvements with caching:

================== ========== ========== ==========
Operation          No Cache   With Cache Speedup
================== ========== ========== ==========
commit_history     2.5s       0.05s      50x
blame              4.2s       0.08s      52x
file_detail        1.8s       0.03s      60x
branches/tags      0.3s       0.01s      30x
bus_factor         3.1s       0.06s      51x
================== ========== ========== ==========

*Benchmarks based on medium-sized repository (~5000 commits, 500 files)*

Data Filtering and Limits
--------------------------

Reduce dataset size to improve performance:

Glob Patterns
~~~~~~~~~~~~~

Use glob patterns to focus analysis on relevant files:

.. code-block:: python

    # Analyze only Python files
    commits = repo.commit_history(include_globs=['*.py'])
    
    # Exclude test and build files
    blame = repo.blame(ignore_globs=['test_*.py', 'build/*', '*.pyc'])
    
    # Multiple patterns
    rates = repo.file_change_rates(
        include_globs=['*.py', '*.js', '*.html'],
        ignore_globs=['*/tests/*', '*/node_modules/*']
    )

Limits and Time Windows
~~~~~~~~~~~~~~~~~~~~~~~

Limit analysis scope for faster results:

.. code-block:: python

    # Limit to recent commits
    recent_commits = repo.commit_history(limit=500)
    
    # Analyze last 90 days only
    recent_changes = repo.file_change_rates(days=90)
    
    # Combine limits with filtering
    python_commits = repo.commit_history(
        limit=1000,
        include_globs=['*.py']
    )

Branch-Specific Analysis
~~~~~~~~~~~~~~~~~~~~~~~~

Analyze specific branches for better performance:

.. code-block:: python

    # Analyze main development branch only
    main_commits = repo.commit_history(branch='main')
    
    # Compare specific branches
    feature_commits = repo.commit_history(branch='feature/new-ui')

Parallelization
---------------

git-pandas uses parallel processing when joblib is available:

Installation
~~~~~~~~~~~~

.. code-block:: bash

    pip install joblib

Parallel Operations
~~~~~~~~~~~~~~~~~~~

Several operations automatically use parallelization:

.. code-block:: python

    from gitpandas import ProjectDirectory

    # Parallel analysis across repositories
    project = ProjectDirectory('/path/to/projects')
    
    # These operations run in parallel when joblib is available:
    commits = project.commit_history()  # Parallel across repos
    branches = project.branches()       # Parallel across repos
    blame = project.cumulative_blame()  # Parallel across commits
    
    # Control parallelization
    blame = repo.parallel_cumulative_blame(
        workers=4,  # Number of parallel workers
        limit=100   # Limit for better performance
    )

Memory Management
-----------------

Optimize memory usage for large repositories:

DataFrame Memory Usage
~~~~~~~~~~~~~~~~~~~~~~

Monitor and optimize DataFrame memory:

.. code-block:: python

    import pandas as pd

    # Check memory usage
    commits = repo.commit_history(limit=10000)
    print(f"Memory usage: {commits.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    
    # Optimize data types
    commits['insertions'] = commits['insertions'].astype('int32')
    commits['deletions'] = commits['deletions'].astype('int32')
    
    # Use categorical for repeated strings
    commits['committer'] = commits['committer'].astype('category')

Chunked Processing
~~~~~~~~~~~~~~~~~~

Process large datasets in chunks:

.. code-block:: python

    def analyze_in_chunks(repo, chunk_size=1000):
        """Analyze repository in chunks to manage memory."""
        all_results = []
        offset = 0
        
        while True:
            # Get chunk of commits
            chunk = repo.commit_history(limit=chunk_size, skip=offset)
            if chunk.empty:
                break
                
            # Process chunk
            result = process_chunk(chunk)
            all_results.append(result)
            
            offset += chunk_size
            
            # Optional: Clear cache periodically
            if offset % 10000 == 0:
                repo.invalidate_cache(pattern='commit_history*')
        
        return pd.concat(all_results, ignore_index=True)

Large Repository Strategies
---------------------------

Special considerations for large repositories:

Repository Size Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~~~

========== ================= =========================
Size       Commits/Files     Recommended Strategy
========== ================= =========================
Small      <1K commits       Any cache, no limits
Medium     1K-10K commits    DiskCache, reasonable limits
Large      10K-100K commits  DiskCache + filtering
Very Large >100K commits     Redis + chunking + limits
========== ================= =========================

Configuration for Large Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Large repository configuration
    cache = DiskCache('/fast/disk/cache.gz', max_keys=10000)
    repo = Repository(
        '/path/to/large/repo',
        cache_backend=cache,
        default_branch='main'
    )
    
    # Use aggressive filtering
    analysis = repo.commit_history(
        limit=5000,  # Reasonable limit
        days=365,    # Last year only
        include_globs=['*.py', '*.js'],  # Core files only
        ignore_globs=['*/tests/*', '*/vendor/*']  # Exclude bulk dirs
    )

Monitoring Performance
~~~~~~~~~~~~~~~~~~~~~~

Track performance metrics:

.. code-block:: python

    import time

    def benchmark_operation(func, *args, **kwargs):
        """Benchmark any git-pandas operation."""
        start_time = time.time()
        start_memory = get_memory_usage()
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_memory = get_memory_usage()
        
        print(f"Execution time: {end_time - start_time:.2f}s")
        print(f"Memory delta: {end_memory - start_memory:.1f}MB")
        print(f"Result size: {len(result)} rows")
        
        return result

    # Example usage
    commits = benchmark_operation(
        repo.commit_history,
        limit=1000,
        include_globs=['*.py']
    )

Performance Anti-Patterns
--------------------------

Avoid these common performance issues:

❌ **No Caching**

.. code-block:: python

    # Slow: No cache means repeated expensive Git operations
    repo = Repository('/path/to/repo')  # No cache_backend
    for branch in ['main', 'develop', 'feature']:
        commits = repo.commit_history(branch=branch)  # Repeated work

✅ **With Caching**

.. code-block:: python

    # Fast: Cache reuses expensive operations
    cache = DiskCache('/tmp/analysis_cache.gz', max_keys=1000)
    repo = Repository('/path/to/repo', cache_backend=cache)
    for branch in ['main', 'develop', 'feature']:
        commits = repo.commit_history(branch=branch)  # Cached after first

❌ **No Filtering**

.. code-block:: python

    # Slow: Processes all files including irrelevant ones
    blame = repo.blame()  # Includes build files, logs, etc.

✅ **With Filtering**

.. code-block:: python

    # Fast: Only analyzes relevant source files
    blame = repo.blame(
        include_globs=['*.py', '*.js', '*.html'],
        ignore_globs=['*/build/*', '*/logs/*', '*.pyc']
    )

❌ **Unlimited Analysis**

.. code-block:: python

    # Slow: Processes entire repository history
    commits = repo.commit_history()  # Could be millions of commits

✅ **Limited Analysis**

.. code-block:: python

    # Fast: Focuses on recent, relevant commits
    commits = repo.commit_history(
        limit=1000,  # Last 1000 commits
        days=90       # Last 90 days
    )

❌ **Memory Leaks**

.. code-block:: python

    # Memory issues: Large DataFrames accumulating
    results = []
    for i in range(100):
        commits = repo.commit_history(limit=10000)
        results.append(commits)  # Accumulating large DataFrames

✅ **Memory Management**

.. code-block:: python

    # Memory efficient: Process and aggregate
    total_commits = 0
    for i in range(100):
        commits = repo.commit_history(limit=1000)  # Smaller chunks
        total_commits += len(commits)
        # Don't accumulate DataFrames
    print(f"Total commits processed: {total_commits}")

Best Practices Summary
----------------------

1. **Always use caching** for any analysis beyond one-off queries
2. **Choose the right cache backend** for your use case:
   - EphemeralCache: Development, interactive analysis
   - DiskCache: Regular workflows, CI/CD
   - RedisDFCache: Team environments, production
3. **Warm your cache** before intensive analysis sessions
4. **Use glob patterns** to filter relevant files only
5. **Set reasonable limits** on commit history and time windows
6. **Monitor cache performance** and invalidate when needed
7. **Profile memory usage** for large repositories
8. **Process in chunks** when dealing with very large datasets
9. **Use parallelization** with ProjectDirectory for multiple repositories
10. **Avoid anti-patterns** that lead to repeated expensive operations

For more detailed examples, see the performance examples in the ``examples/`` directory:

- ``examples/cache_management.py`` - Cache management and monitoring
- ``examples/remote_fetch_and_cache_warming.py`` - Cache warming strategies
- ``examples/parallel_blame.py`` - Parallel processing examples