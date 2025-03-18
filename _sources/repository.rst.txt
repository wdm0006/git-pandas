Repository
=========

The Repository class provides a powerful interface for analyzing a single Git repository. It can be created from either a local or remote repository.

Overview
--------

The Repository class offers methods for:
* Extracting commit history and file changes
* Analyzing blame information
* Tracking branches and tags
* Generating cumulative blame statistics
* Calculating file ownership and contribution patterns

Creating a Repository
-------------------

You can create a Repository object in two ways:

Local Repository
~~~~~~~~~~~~~~

Create a Repository from a local Git repository:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository(working_dir='/path/to/repo/', verbose=True)

The directory must contain a `.git` directory. Subdirectories are not searched.

Remote Repository
~~~~~~~~~~~~~~~

Create a Repository from a remote Git repository:

.. code-block:: python

    from gitpandas import Repository
    repo = Repository(working_dir='git://github.com/user/repo.git', verbose=True)

The repository will be cloned locally into a temporary directory. This can be slow for large repositories.

Common Operations
---------------

Here are some common operations you can perform with a Repository object:

.. code-block:: python

    # Get commit history
    commits_df = repo.commit_history()
    
    # Get blame information
    blame_df = repo.blame()
    
    # Get branch information
    branches_df = repo.branches()
    
    # Get tag information
    tags_df = repo.tags()
    
    # Get file change history
    changes_df = repo.file_change_history()

API Reference
------------

.. automodule:: gitpandas.repository
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members: