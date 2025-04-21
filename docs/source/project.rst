Project Directory
=================

The ProjectDirectory class enables analysis across multiple Git repositories. It can aggregate metrics and insights from multiple repositories into a single output.

Overview
--------

The ProjectDirectory class provides:

* Analysis across multiple repositories
* Aggregated metrics and statistics
* Project-level insights
* Multi-repository bus factor analysis
* Consolidated commit history and blame information

Creating a ProjectDirectory
---------------------------

You can create a ProjectDirectory object in three ways:

Directory of Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ProjectDirectory from a directory containing multiple repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory(
        working_dir='/path/to/dir/',
        ignore_repos=['repo_to_ignore'],
        verbose=True,
        default_branch='main'  # Optional, will auto-detect if not specified
    )

The `ignore_repos` parameter can be a list of repository names to exclude. This method uses `os.walk` to search for `.git` directories recursively.

Explicit Local Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ProjectDirectory from a list of local repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory(
        working_dir=['/path/to/repo1/', '/path/to/repo2/'],
        ignore_repos=['repo_to_ignore'],
        verbose=True,
        default_branch='main'  # Optional, will auto-detect if not specified
    )

Explicit Remote Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ProjectDirectory from a list of remote repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory(
        working_dir=['git://github.com/user/repo1.git', 'git://github.com/user/repo2.git'],
        ignore_repos=['repo_to_ignore'],
        verbose=True,
        default_branch='main'  # Optional, will auto-detect if not specified
    )

Available Methods
----------------

Core Analysis
~~~~~~~~~~~~

.. code-block:: python

    # Commit history across repositories
    project.commit_history(
        branch=None,          # Branch to analyze
        limit=None,           # Maximum number of commits
        days=None,           # Limit to last N days
        ignore_globs=None,   # Files to ignore
        include_globs=None   # Files to include
    )

    # File change history across repositories
    project.file_change_history(
        branch=None,
        limit=None,
        days=None,
        ignore_globs=None,
        include_globs=None
    )

    # Blame analysis across repositories
    project.blame(
        rev="HEAD",          # Revision to analyze
        committer=True,      # Group by committer (False for author)
        by="repository",     # Group by 'repository' or 'file'
        ignore_globs=None,
        include_globs=None
    )

    # Bus factor analysis across repositories
    project.bus_factor(
        by="repository",     # How to group results
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

.. currentmodule:: gitpandas.project

.. autoclass:: ProjectDirectory
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members:
   :special-members: __init__, __str__, __repr__

   .. rubric:: Methods
