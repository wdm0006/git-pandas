.. git-pandas documentation master file, created by
   sphinx-quickstart on Sun Nov  8 21:21:04 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Git-Pandas Documentation
===================================

Git-Pandas is a Python library that transforms Git repository data into pandas DataFrames, making it easy to analyze your codebase's history and development patterns. Built on top of GitPython, it provides a simple interface for extracting insights from your Git repositories.

.. image:: https://raw.githubusercontent.com/wdm0006/git-pandas/master/examples/img/githubblame.png
   :alt: Cumulative Blame Visualization
   :align: center

Quick Start
-----------

Install Git-Pandas using pip:

.. code-block:: bash

    pip install git-pandas

Basic Usage
~~~~~~~~~~~

Analyze a single repository:

.. code-block:: python

    from gitpandas import Repository
    
    # Create a repository instance
    repo = Repository('/path/to/repo')
    
    # Get commit history with filtering
    commits_df = repo.commit_history(
        branch='main',
        ignore_globs=['*.pyc'],
        include_globs=['*.py']
    )
    
    # Analyze blame information
    blame_df = repo.blame(by='repository')
    
    # Calculate bus factor
    bus_factor_df = repo.bus_factor()

Analyze multiple repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory('/path/to/project')

Key Features
------------

* **Repository Analysis**: Extract commit history, file changes, and blame information
* **Project Insights**: Calculate bus factor and analyze repository metrics
* **Multi-Repository Support**: Analyze multiple repositories together
* **Remote Operations**: Safely fetch changes from remote repositories
* **Cache Warming**: Pre-populate caches for improved performance
* **Bulk Operations**: Efficiently process multiple repositories in parallel
* **Performance Optimization**: Advanced caching support and glob-based filtering

Core Components
---------------

The library is built around two main components:

Repository
~~~~~~~~~~
A wrapper around a single Git repository that provides:

* Commit history analysis with filtering options
* File change tracking and blame information
* Branch existence checking and repository status
* Bus factor calculation and repository metrics
* Punchcard statistics generation

ProjectDirectory
~~~~~~~~~~~~~~~
A collection of Git repositories that enables:

* Analysis across multiple repositories
* Aggregated metrics and statistics
* Project-level insights

Common Parameters
----------------

Most analysis methods support these filtering parameters:

* **branch**: Branch to analyze (defaults to repository's default branch)
* **limit**: Maximum number of commits to analyze
* **days**: Limit analysis to last N days
* **ignore_globs**: List of glob patterns for files to ignore
* **include_globs**: List of glob patterns for files to include
* **by**: How to group results (usually 'repository' or 'file')

Documentation
-------------

For detailed information about the components and their usage, see:

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   repository
   project
   cache
   remote_operations
   performance
   usecases
   contributors

Additional Resources
--------------------

* :ref:`genindex` - Complete API reference
* :ref:`modindex` - Module index
* :ref:`search` - Search the documentation

License
-------

This project is BSD licensed (see LICENSE.md)

