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

Available Cache Backends
----------------------

In-Memory Cache
~~~~~~~~~~~~~

The default in-memory cache is ephemeral and will be cleared when the process ends:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository('/path/to/repo', cache=True)  # Enable in-memory caching

Redis Cache
~~~~~~~~~

For persistent caching across sessions, use Redis:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository(
        '/path/to/repo',
        cache=True,
        cache_backend='redis',
        redis_url='redis://localhost:6379/0'
    )

Configuration
------------

You can configure cache behavior through the following parameters:

* ``cache``: Enable/disable caching (default: False)
* ``cache_backend``: Choose the cache backend ('memory' or 'redis')
* ``cache_ttl``: Time-to-live for cached results in seconds
* ``redis_url``: Redis connection URL (for Redis backend)

Example Usage
------------

.. code-block:: python

    from gitpandas import Repository
    
    # Create repository with Redis caching
    repo = Repository(
        working_dir='/path/to/repo',
        cache=True,
        cache_backend='redis',
        cache_ttl=3600,  # Cache for 1 hour
        redis_url='redis://localhost:6379/0'
    )
    
    # First call will compute and cache the result
    blame_df = repo.blame()
    
    # Subsequent calls will use cached result if available
    blame_df = repo.blame()  # Much faster!

API Reference
------------

.. automodule:: gitpandas.cache
   :members:
   :undoc-members:
   :show-inheritance:
