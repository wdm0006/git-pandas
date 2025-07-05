Use Cases and Examples
======================

Git-Pandas provides a powerful interface for analyzing Git repositories using pandas DataFrames. This guide demonstrates common use cases and provides practical examples.

Basic Repository Analysis
-------------------------

Repository Attributes
~~~~~~~~~~~~~~~~~~~~~

Get basic information about a repository:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository('/path/to/repo')
    
    # Get repository name
    print(repo.repo_name)
    
    # Check if repository is bare
    print(repo.is_bare())
    
    # Get all tags
    print(repo.tags())
    
    # Get all branches
    print(repo.branches())
    
    # Get all revisions
    print(repo.revs())
    
    # Get blame information
    print(repo.blame(include_globs=['*.py']))

Commit History Analysis
~~~~~~~~~~~~~~~~~~~~~~~

Analyze commit patterns and history:

.. code-block:: python

    # Get commit history
    commits_df = repo.commit_history()
    
    # Get file change history
    changes_df = repo.file_change_history()
    
    # Filter by file extension
    python_changes = repo.file_change_history(include_globs=['*.py'])
    
    # Filter by directory
    src_changes = repo.file_change_history(include_globs=['src/*'])
    
    # Get commits in tags
    tag_commits = repo.commits_in_tags()

Project-Level Analysis
----------------------

Multiple Repository Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Analyze multiple repositories simultaneously:

.. code-block:: python

    from gitpandas import ProjectDirectory
    
    # Create project from multiple repositories
    project = ProjectDirectory([
        'git://github.com/user/repo1.git',
        'git://github.com/user/repo2.git'
    ])
    
    # Get repository information
    print(project.repo_information())
    
    # Calculate bus factor
    print(project.bus_factor())
    
    # Get file change history
    print(project.file_change_history())
    
    # Get blame information
    print(project.blame())

Advanced Analysis
-----------------

Cumulative Blame Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~

Track code ownership over time:

.. code-block:: python

    # Get cumulative blame
    blame_df = repo.cumulative_blame()
    
    # Plot cumulative blame using pandas plotting
    import matplotlib.pyplot as plt
    blame_df.plot(x='date', y='loc', title='Cumulative Blame Over Time')
    plt.show()

Bus Factor Analysis
~~~~~~~~~~~~~~~~~~~

Analyze project sustainability:

.. code-block:: python

    # Calculate bus factor for repository
    bus_factor = repo.bus_factor()
    
    # Calculate file-wise bus factor (new in v2.5.0)
    file_bus_factor = repo.bus_factor(by='file')
    
    # Get detailed blame information
    blame_df = repo.blame(by='file')  # Get file-level blame details
    
    # Analyze ownership patterns
    ownership_patterns = repo.blame(committer=True, by='repository')

Performance Optimization
------------------------

Using Caching
~~~~~~~~~~~~~

Optimize performance with caching:

.. code-block:: python

    from gitpandas import Repository
    from gitpandas.cache import EphemeralCache, DiskCache, RedisDFCache
    
    # Use in-memory caching
    cache = EphemeralCache(max_keys=1000)
    repo = Repository('/path/to/repo', cache_backend=cache)
    
    # Use persistent disk caching (new in v2.5.0)
    disk_cache = DiskCache('/tmp/gitpandas_cache.gz', max_keys=500)
    repo = Repository('/path/to/repo', cache_backend=disk_cache)
    
    # Or use Redis for distributed caching
    redis_cache = RedisDFCache(
        host='localhost',
        port=6379,
        db=12,
        ttl=3600  # Cache entries expire after 1 hour
    )
    repo = Repository('/path/to/repo', cache_backend=redis_cache)

Cache Management (New in v2.5.0)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Manage cache performance and contents:

.. code-block:: python

    # Get cache statistics
    stats = repo.get_cache_stats()
    print(f"Cache usage: {stats['global_cache_stats']['cache_usage_percent']:.1f}%")
    
    # Invalidate specific cache entries
    repo.invalidate_cache(keys=['commit_history'])
    
    # Clear all cache for this repository
    repo.invalidate_cache()
    
    # Warm cache for better performance
    result = repo.warm_cache(methods=['commit_history', 'blame'], limit=100)
    print(f"Created {result['cache_entries_created']} cache entries")

Visualization Examples
----------------------

Commit Analysis
~~~~~~~~~~~~~~~

Visualize commit patterns:

.. code-block:: python

    # Get commit history
    commit_df = repo.commit_history()
    
    # Plot commits over time using pandas
    commit_df.resample('D').size().plot(
        kind='bar',
        title='Commits per Day'
    )
    plt.show()

File Change Analysis
~~~~~~~~~~~~~~~~~~~~

Visualize file changes:

.. code-block:: python

    # Get file change history
    changes_df = repo.file_change_history()
    
    # Plot changes over time using pandas
    changes_df.groupby('filename')['insertions'].sum().plot(
        kind='bar',
        title='Lines Added by File'
    )
    plt.show()

Best Practices
--------------

* Use caching for expensive operations like blame analysis
* Filter data early using include_globs/ignore_globs
* Leverage pandas operations for analysis
* Consider memory usage with large repositories
* Use appropriate branch names (main/master)
* Handle repository cleanup properly when using remote repositories

For more examples and detailed API documentation, see the :doc:`repository` and :doc:`project` pages.
