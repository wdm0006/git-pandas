Project Directory
=================

The ProjectDirectory class enables analysis across multiple Git repositories. It can aggregate metrics and insights from multiple repositories into a single output.

Overview
--------

The ProjectDirectory class provides:
* Aggregation of metrics across multiple repositories
* Project-level insights and statistics
* Cross-repository analysis capabilities
* Development time estimation
* Bus factor calculation

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

To check which repositories are included:

.. code-block:: python

    print(project.repo_name())

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

Common Operations
-----------------

Here are some common operations you can perform with a ProjectDirectory object:

.. code-block:: python

    # Get commit history across all repositories
    commits_df = project.commit_history()
    
    # Get blame information across all repositories
    blame_df = project.blame()
    
    # Get branch information across all repositories
    branches_df = project.branches()
    
    # Get tag information across all repositories
    tags_df = project.tags()
    
    # Get file change history across all repositories
    changes_df = project.file_change_history()
    
    # Get detailed repository information
    repo_info = project.repo_information()

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
