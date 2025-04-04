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

RedisDFCache:
* ``host``: Redis host (default: 'localhost')
* ``port``: Redis port (default: 6379)
* ``db``: Redis database number (default: 12)
* ``max_keys``: Maximum number of keys to store (default: 1000)
* ``ttl``: Time-to-live in seconds for cache entries (default: None, no expiration)
* Additional keyword arguments are passed to redis.StrictRedis

API Reference
-------------

.. currentmodule:: gitpandas.cache

.. autoclass:: EphemeralCache
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
