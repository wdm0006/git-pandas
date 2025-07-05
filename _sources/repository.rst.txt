Repository
==========

The Repository class provides an interface for analyzing a single Git repository. It can be created from either a local or remote repository.

Overview
--------

The Repository class offers methods for:

* Commit history analysis with filtering options
* File change tracking and blame information
* Branch existence checking and repository status
* Bus factor calculation and repository metrics
* Punchcard statistics generation

Creating a Repository
---------------------

You can create a Repository object in two ways:

Local Repository
~~~~~~~~~~~~~~~~

Create a Repository from a local Git repository:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository(
        working_dir='/path/to/repo/',
        verbose=True,
        default_branch='main'  # Optional, will auto-detect if not specified
    )

The directory must contain a `.git` directory. Subdirectories are not searched.

Remote Repository
~~~~~~~~~~~~~~~~~

Create a Repository from a remote Git repository:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository(
        working_dir='git://github.com/user/repo.git',
        verbose=True,
        default_branch='main'  # Optional, will auto-detect if not specified
    )

The repository will be cloned locally into a temporary directory. This can be slow for large repositories.

Available Methods
----------------

Core Analysis
~~~~~~~~~~~~

.. code-block:: python

    # Commit history analysis
    repo.commit_history(
        branch=None,          # Branch to analyze
        limit=None,           # Maximum number of commits
        days=None,           # Limit to last N days
        ignore_globs=None,   # Files to ignore
        include_globs=None   # Files to include
    )

    # File change history
    repo.file_change_history(
        branch=None,
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None
    )

    # Blame analysis
    repo.blame(
        rev="HEAD",          # Revision to analyze
        committer=True,      # Group by committer (False for author)
        by="repository",     # Group by 'repository' or 'file'
        ignore_globs=None,
        include_globs=None
    )

    # Bus factor analysis
    repo.bus_factor(
        by="repository",     # How to group results ('repository' or 'file')
        ignore_globs=None,
        include_globs=None
    )

    # Commit pattern analysis
    repo.punchcard(
        branch=None,
        limit=None,
        days=None,
        by=None,            # Additional grouping
        normalize=None,     # Normalize values
        ignore_globs=None,
        include_globs=None
    )

Repository Information
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # List files in repository
    repo.list_files(rev="HEAD")

    # Check branch existence
    repo.has_branch(branch)

    # Check if repository is bare
    repo.is_bare()

    # Check for coverage information
    repo.has_coverage()
    repo.coverage()

    # Get specific commit content
    repo.get_commit_content(
        rev,                # Revision to analyze
        ignore_globs=None,
        include_globs=None
    )

Common Parameters
----------------

Most analysis methods support these filtering parameters:

* **branch**: Branch to analyze (defaults to repository's default branch)
* **limit**: Maximum number of commits to analyze
* **days**: Limit analysis to last N days
* **ignore_globs**: List of glob patterns for files to ignore
* **include_globs**: List of glob patterns for files to include
* **by**: How to group results (usually 'repository' or 'file')

API Reference
-------------

.. currentmodule:: gitpandas.repository

.. autoclass:: Repository
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:
   :special-members: __init__, __str__, __repr__

.. autoclass:: GitFlowRepository
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members: