Remote Operations and Cache Warming
===================================

Git-pandas provides safe and efficient methods for working with remote repositories and optimizing performance through cache warming. These features allow you to keep your repositories up to date and improve analysis performance through intelligent caching.

Safe Remote Fetch
-----------------

The ``safe_fetch_remote`` method allows you to safely fetch changes from remote repositories without modifying your working directory or current branch.

Repository.safe_fetch_remote()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: gitpandas.repository.Repository.safe_fetch_remote

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import EphemeralCache

    # Create repository with caching
    cache = EphemeralCache(max_keys=100)
    repo = Repository('/path/to/repo', cache_backend=cache)

    # Perform a dry run to see what would be fetched
    dry_result = repo.safe_fetch_remote(dry_run=True)
    print(f"Would fetch from: {dry_result['message']}")

    # Safely fetch changes
    if dry_result['remote_exists']:
        result = repo.safe_fetch_remote()
        if result['success']:
            print(f"Fetch completed: {result['message']}")
            if result['changes_available']:
                print("New changes are available!")
        else:
            print(f"Fetch failed: {result['error']}")

Advanced Options
^^^^^^^^^^^^^^^^

.. code-block:: python

    # Fetch from a specific remote
    result = repo.safe_fetch_remote(remote_name='upstream')

    # Fetch and prune deleted remote branches
    result = repo.safe_fetch_remote(prune=True)

    # Perform dry run to preview without fetching
    result = repo.safe_fetch_remote(dry_run=True)

Safety Features
^^^^^^^^^^^^^^^

- **Read-only operation**: Never modifies working directory or current branch
- **Error handling**: Gracefully handles network errors and missing remotes
- **Validation**: Checks for remote existence before attempting fetch
- **Dry run support**: Preview operations without making changes

Cache Warming
-------------

Cache warming pre-populates the cache with commonly used data to improve performance of subsequent analysis operations.

Repository.warm_cache()
~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: gitpandas.repository.Repository.warm_cache

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import DiskCache

    # Create repository with persistent cache
    cache = DiskCache('/tmp/my_cache.gz', max_keys=200)
    repo = Repository('/path/to/repo', cache_backend=cache)

    # Warm cache with default methods
    result = repo.warm_cache()
    print(f"Cache warming completed in {result['execution_time']:.2f} seconds")
    print(f"Created {result['cache_entries_created']} cache entries")
    print(f"Methods executed: {result['methods_executed']}")

Custom Cache Warming
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Warm specific methods with custom parameters
    result = repo.warm_cache(
        methods=['commit_history', 'blame', 'file_detail'],
        limit=100,
        branch='main',
        ignore_globs=['*.log', '*.tmp']
    )

    # Check results
    if result['success']:
        print(f"Successfully warmed {len(result['methods_executed'])} methods")
    else:
        print(f"Errors occurred: {result['errors']}")

Available Methods
^^^^^^^^^^^^^^^^^

The following methods can be warmed:

- ``commit_history``: Load commit history
- ``branches``: Load branch information  
- ``tags``: Load tag information
- ``blame``: Load blame information
- ``file_detail``: Load file details
- ``list_files``: Load file listing
- ``file_change_rates``: Load file change statistics

Performance Benefits
^^^^^^^^^^^^^^^^^^^^

Cache warming can significantly improve performance:

.. code-block:: python

    import time

    # Test cold performance
    start = time.time()
    history_cold = repo.commit_history(limit=100)
    cold_time = time.time() - start

    # Warm the cache
    repo.warm_cache(methods=['commit_history'], limit=100)

    # Test warm performance
    start = time.time()
    history_warm = repo.commit_history(limit=100)
    warm_time = time.time() - start

    speedup = cold_time / warm_time
    print(f"Cache warming provided {speedup:.1f}x speedup!")

Bulk Operations
---------------

For projects with multiple repositories, bulk operations allow you to efficiently fetch and warm caches across all repositories.

ProjectDirectory.bulk_fetch_and_warm()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: gitpandas.project.ProjectDirectory.bulk_fetch_and_warm

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from gitpandas import ProjectDirectory
    from gitpandas.cache import DiskCache

    # Create project directory with shared cache
    cache = DiskCache('/tmp/project_cache.gz', max_keys=500)
    project = ProjectDirectory('/path/to/repos', cache_backend=cache)

    # Perform bulk operations
    result = project.bulk_fetch_and_warm(
        fetch_remote=True,
        warm_cache=True,
        parallel=True
    )

    print(f"Processed {result['repositories_processed']} repositories")
    print(f"Fetch summary: {result['summary']['fetch_successful']} successful")
    print(f"Cache summary: {result['summary']['cache_successful']} successful")

Advanced Bulk Operations
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Customize bulk operations
    result = project.bulk_fetch_and_warm(
        fetch_remote=True,
        warm_cache=True,
        parallel=True,
        remote_name='upstream',
        prune=True,
        dry_run=False,
        cache_methods=['commit_history', 'blame'],
        limit=200,
        ignore_globs=['*.log']
    )

    # Check individual repository results
    for repo_name, fetch_result in result['fetch_results'].items():
        if not fetch_result['success']:
            print(f"Fetch failed for {repo_name}: {fetch_result['error']}")

    for repo_name, cache_result in result['cache_results'].items():
        print(f"{repo_name}: {cache_result['cache_entries_created']} cache entries")

Parallel Processing
^^^^^^^^^^^^^^^^^^^

Bulk operations support parallel processing when ``joblib`` is available:

.. code-block:: python

    # Enable parallel processing (default when joblib available)
    result = project.bulk_fetch_and_warm(
        fetch_remote=True,
        warm_cache=True,
        parallel=True  # Uses all available CPU cores
    )

    # Disable parallel processing for sequential execution
    result = project.bulk_fetch_and_warm(
        fetch_remote=True,
        warm_cache=True,
        parallel=False
    )

Best Practices
--------------

Repository Management
^^^^^^^^^^^^^^^^^^^^

1. **Regular Fetching**: Use ``safe_fetch_remote`` regularly to keep repositories current
2. **Dry Run First**: Use dry runs to preview fetch operations
3. **Error Handling**: Always check return values for errors
4. **Remote Validation**: Verify remotes exist before fetching

Cache Optimization
^^^^^^^^^^^^^^^^^

1. **Persistent Caching**: Use ``DiskCache`` for long-term cache persistence
2. **Appropriate Cache Size**: Set reasonable ``max_keys`` based on your usage
3. **Selective Warming**: Only warm methods you actually use
4. **Regular Warming**: Re-warm caches when data becomes stale

Bulk Operations
^^^^^^^^^^^^^^

1. **Shared Caches**: Use shared cache backends across repositories
2. **Parallel Processing**: Enable parallel processing for multiple repositories
3. **Custom Parameters**: Tailor operations to your specific needs
4. **Error Isolation**: Handle errors at the repository level

Error Handling
--------------

All remote operations and cache warming methods provide comprehensive error information:

.. code-block:: python

    # Safe fetch error handling
    result = repo.safe_fetch_remote()
    if not result['success']:
        if result['remote_exists']:
            print(f"Fetch failed: {result['error']}")
        else:
            print(f"No remote configured: {result['message']}")

    # Cache warming error handling
    result = repo.warm_cache()
    if not result['success']:
        print(f"Failed methods: {result['methods_failed']}")
        for error in result['errors']:
            print(f"Error: {error}")

    # Bulk operation error handling
    result = project.bulk_fetch_and_warm(fetch_remote=True, warm_cache=True)
    for repo_name, repo_result in result['fetch_results'].items():
        if not repo_result['success']:
            print(f"Repository {repo_name} failed: {repo_result.get('error', 'Unknown error')}")

Examples
--------

Complete examples demonstrating these features can be found in the ``examples/`` directory:

- ``examples/remote_fetch_and_cache_warming.py``: Comprehensive demonstration of all features
- ``examples/cache_timestamps.py``: Cache timestamp and metadata examples

Return Value Reference
---------------------

safe_fetch_remote Return Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``safe_fetch_remote`` method returns a dictionary with these keys:

- ``success`` (bool): Whether the fetch was successful
- ``message`` (str): Status message or description
- ``remote_exists`` (bool): Whether the specified remote exists
- ``changes_available`` (bool): Whether new changes were fetched
- ``error`` (str or None): Error message if fetch failed

warm_cache Return Values
^^^^^^^^^^^^^^^^^^^^^^^

The ``warm_cache`` method returns a dictionary with these keys:

- ``success`` (bool): Whether cache warming was successful
- ``methods_executed`` (list): List of methods that were executed
- ``methods_failed`` (list): List of methods that failed
- ``cache_entries_created`` (int): Number of cache entries created
- ``execution_time`` (float): Total execution time in seconds
- ``errors`` (list): List of error messages for failed methods

bulk_fetch_and_warm Return Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``bulk_fetch_and_warm`` method returns a dictionary with these keys:

- ``success`` (bool): Whether the overall operation was successful
- ``repositories_processed`` (int): Number of repositories processed
- ``fetch_results`` (dict): Per-repository fetch results
- ``cache_results`` (dict): Per-repository cache warming results
- ``execution_time`` (float): Total execution time in seconds
- ``summary`` (dict): Summary statistics including:
  
  - ``fetch_successful`` (int): Number of successful fetches
  - ``fetch_failed`` (int): Number of failed fetches
  - ``cache_successful`` (int): Number of successful cache warming operations
  - ``cache_failed`` (int): Number of failed cache warming operations
  - ``repositories_with_remotes`` (int): Number of repositories with remotes
  - ``total_cache_entries_created`` (int): Total cache entries created across all repositories