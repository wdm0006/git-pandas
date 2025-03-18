Use Cases and Examples
=====================

Git-Pandas provides a powerful interface for analyzing Git repositories using pandas DataFrames. This guide demonstrates common use cases and provides practical examples.

Basic Repository Analysis
------------------------

Repository Attributes
~~~~~~~~~~~~~~~~~~

Get basic information about a repository:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository('/path/to/repo')
    
    # Get repository name
    print(repo._repo_name())
    
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
~~~~~~~~~~~~~~~~~~~~

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

Project-Level Analysis
--------------------

Multiple Repository Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~

Analyze multiple repositories simultaneously:

.. code-block:: python

    from gitpandas import ProjectDirectory
    
    # Create project from multiple repositories
    project = ProjectDirectory([
        'git://github.com/user/repo1.git',
        'git://github.com/user/repo2.git'
    ])
    
    # Get aggregated metrics
    print(project.general_information())
    
    # Calculate bus factor
    print(project.bus_factor())
    
    # Get file change rates
    print(project.file_change_rates())
    
    # Generate punchcard data
    print(project.punchcard())

Advanced Analysis
---------------

Cumulative Blame Analysis
~~~~~~~~~~~~~~~~~~~~~~~

Track code ownership over time:

.. code-block:: python

    # Get cumulative blame
    blame_df = repo.cumulative_blame()
    
    # Plot cumulative blame
    import matplotlib.pyplot as plt
    blame_df.plot(x='date', y='loc', title='Cumulative Blame Over Time')
    plt.show()

Bus Factor Analysis
~~~~~~~~~~~~~~~~~

Analyze project sustainability:

.. code-block:: python

    # Calculate bus factor
    bus_factor = project.bus_factor()
    
    # Get detailed contributor metrics
    contributors_df = project.contributor_metrics()
    
    # Analyze file ownership
    ownership_df = project.file_ownership()

Performance Optimization
---------------------

Using Caching
~~~~~~~~~~~

Optimize performance with caching:

.. code-block:: python

    # Enable in-memory caching
    repo = Repository('/path/to/repo', cache=True)
    
    # Use Redis for persistent caching
    repo = Repository(
        '/path/to/repo',
        cache=True,
        cache_backend='redis',
        redis_url='redis://localhost:6379/0'
    )

Visualization Examples
-------------------

Commit Patterns
~~~~~~~~~~~~~

Visualize commit patterns:

.. code-block:: python

    # Generate punchcard data
    punchcard_df = repo.punchcard()
    
    # Plot commit patterns
    import matplotlib.pyplot as plt
    punchcard_df.plot(kind='heatmap', title='Commit Patterns')
    plt.show()

File Change Analysis
~~~~~~~~~~~~~~~~~

Visualize file changes:

.. code-block:: python

    # Get file change history
    changes_df = repo.file_change_history()
    
    # Plot changes over time
    changes_df.plot(x='date', y='changes', title='File Changes Over Time')
    plt.show()

Best Practices
------------

* Use caching for expensive operations
* Filter data early to improve performance
* Leverage pandas operations for analysis
* Consider memory usage with large repositories
* Use appropriate visualization tools

For more examples and detailed API documentation, see the :doc:`repository` and :doc:`project` pages.
