Caching
=======

Git-Pandas supports pluggable cache backends to optimize performance for expensive, repetitive operations. This is particularly useful for large repositories or when running multiple analyses.

Overview
--------

The caching system provides:
* In-memory caching for temporary results
* Redis-based caching for persistent storage
* Configurable cache durations
* Automatic cache invalidation
* Decorator-based caching for expensive operations
* **Cache timestamp tracking** - know when cache entries were populated

Available Cache Backends
------------------------

In-Memory Cache (EphemeralCache)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default in-memory cache is ephemeral and will be cleared when the process ends:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import EphemeralCache
    
    # Create an in-memory cache with default settings
    cache = EphemeralCache()
    
    # Or customize the cache size
    cache = EphemeralCache(max_keys=500)
    
    # Use the cache with a repository
    repo = Repository('/path/to/repo', cache_backend=cache)

Disk Cache (DiskCache)
~~~~~~~~~~~~~~~~~~~~~~

For persistent caching that survives between sessions:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import DiskCache
    
    # Create a disk cache
    cache = DiskCache(filepath='/path/to/cache.gz', max_keys=1000)
    
    # Use the cache with a repository
    repo = Repository('/path/to/repo', cache_backend=cache)

Redis Cache (RedisDFCache)
~~~~~~~~~~~~~~~~~~~~~~~~~~

For persistent caching across sessions, use Redis:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import RedisDFCache
    
    # Create a Redis cache with default settings
    cache = RedisDFCache()
    
    # Or customize Redis connection and cache settings
    cache = RedisDFCache(
        host='localhost',
        port=6379,
        db=12,
        max_keys=1000,
        ttl=3600  # Cache entries expire after 1 hour
    )
    
    # Use the cache with a repository
    repo = Repository('/path/to/repo', cache_backend=cache)

Cache Timestamp Information
---------------------------

All cache backends now track when cache entries were populated. You can access this information
without any changes to the Repository or ProjectDirectory API:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import EphemeralCache
    
    # Create repository with cache
    cache = EphemeralCache()
    repo = Repository('/path/to/repo', cache_backend=cache)
    
    # Populate cache with some operations
    commit_history = repo.commit_history(limit=10)
    file_list = repo.list_files()
    
    # Check what's in the cache and when it was cached
    cached_keys = cache.list_cached_keys()
    for entry in cached_keys:
        print(f"Key: {entry['key']}")
        print(f"Cached at: {entry['cached_at']}")
        print(f"Age: {entry['age_seconds']:.1f} seconds")
    
    # Get specific cache information
    key = "commit_history_main_10_None_None_None_None"
    info = cache.get_cache_info(key)
    if info:
        print(f"Cache entry age: {info['age_minutes']:.2f} minutes")

Cache Information Methods
~~~~~~~~~~~~~~~~~~~~~~~~~

All cache backends support these methods for accessing timestamp information:

* ``list_cached_keys()`` - Returns list of all cached keys with metadata
* ``get_cache_info(key)`` - Returns detailed information about a specific cache entry

The returned information includes:

* ``cached_at`` - UTC timestamp when the entry was cached
* ``age_seconds`` - Age of the cache entry in seconds
* ``age_minutes`` - Age of the cache entry in minutes  
* ``age_hours`` - Age of the cache entry in hours
* ``cache_key`` - The original cache key

Using the Cache Decorator
-------------------------

The `@multicache` decorator can be used to cache method results:

.. code-block:: python

    from gitpandas.cache import multicache
    
    @multicache(
        key_prefix="method_name",
        key_list=["param1", "param2"],
        skip_if=lambda x: x.get("param1") is None
    )
    def expensive_method(self, param1, param2):
        # Method implementation
        pass

Configuration
-------------

Cache backends can be configured with various parameters:

EphemeralCache:
* ``max_keys``: Maximum number of keys to store in memory (default: 1000)

DiskCache:
* ``filepath``: Path to the cache file (required)
* ``max_keys``: Maximum number of keys to store (default: 1000)

RedisDFCache:
* ``host``: Redis host (default: 'localhost')
* ``port``: Redis port (default: 6379)
* ``db``: Redis database number (default: 12)
* ``max_keys``: Maximum number of keys to store (default: 1000)
* ``ttl``: Time-to-live in seconds for cache entries (default: None, no expiration)
* Additional keyword arguments are passed to redis.StrictRedis

Backward Compatibility
----------------------

The cache timestamp functionality is fully backward compatible:

* Existing cache files will continue to work
* Old cache entries without timestamps will be automatically converted
* No changes to Repository or ProjectDirectory APIs
* All existing code continues to work unchanged

Best Practices
--------------

Shared Cache Usage
~~~~~~~~~~~~~~~~~~

.. warning::
   **Recommendation: Use Separate Cache Instances**
   
   While it's technically possible to share the same cache object across multiple Repository instances, 
   we **strongly recommend using separate cache instances** for each repository for the following reasons:

**Recommended Approach - Separate Caches:**

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import DiskCache
    
    # Create separate cache instances for each repository
    cache1 = DiskCache(filepath='repo1_cache.gz')
    cache2 = DiskCache(filepath='repo2_cache.gz')
    
    repo1 = Repository('/path/to/repo1', cache_backend=cache1)
    repo2 = Repository('/path/to/repo2', cache_backend=cache2)

**Benefits of Separate Caches:**

* **Complete Isolation**: No risk of cache eviction conflicts between repositories
* **Predictable Memory Usage**: Each repository has its own memory budget
* **Easier Debugging**: Cache issues are isolated to specific repositories  
* **Better Performance**: No lock contention in multi-threaded scenarios
* **Clear Cache Management**: You can clear or manage each repository's cache independently

**If You Must Share Caches:**

If you need to share a cache object across multiple repositories (e.g., for memory constraints), 
the system is designed to handle this safely:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import EphemeralCache
    
    # Shared cache (not recommended but supported)
    shared_cache = EphemeralCache(max_keys=1000)
    
    repo1 = Repository('/path/to/repo1', cache_backend=shared_cache)
    repo2 = Repository('/path/to/repo2', cache_backend=shared_cache)
    
    # Each repository gets separate cache entries
    files1 = repo1.list_files()  # Creates cache key: list_files||repo1||None
    files2 = repo2.list_files()  # Creates cache key: list_files||repo2||None

**Shared Cache Considerations:**

* Repository names are included in cache keys to prevent collisions
* Cache eviction affects all repositories sharing the cache
* Memory usage is shared across all repositories
* Very active repositories may evict cache entries from less active ones

Cache Size Planning
~~~~~~~~~~~~~~~~~~~

When planning cache sizes, consider:

* **Repository Size**: Larger repositories generate more cache entries
* **Operation Types**: Some operations (like ``cumulative_blame``) create many cache entries
* **Memory Constraints**: Balance cache size with available system memory
* **Analysis Patterns**: Frequently repeated analyses benefit from larger caches

**Recommended Cache Sizes:**

.. code-block:: python

    # Small repositories (< 1000 commits)
    cache = EphemeralCache(max_keys=100)
    
    # Medium repositories (1000-10000 commits)  
    cache = EphemeralCache(max_keys=500)
    
    # Large repositories (> 10000 commits)
    cache = EphemeralCache(max_keys=1000)
    
    # For disk/Redis caches, you can use larger sizes
    cache = DiskCache(filepath='cache.gz', max_keys=5000)

API Reference
-------------

.. currentmodule:: gitpandas.cache

.. autoclass:: EphemeralCache
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:
   :special-members: __init__

.. autoclass:: DiskCache
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:
   :special-members: __init__

.. autoclass:: RedisDFCache
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:
   :special-members: __init__

.. autofunction:: multicache

.. autoclass:: CacheEntry
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
