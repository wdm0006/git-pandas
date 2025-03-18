Project Directory
===============

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
-------------------------

You can create a ProjectDirectory object in three ways:

Directory of Repositories
~~~~~~~~~~~~~~~~~~~~~~~

Create a ProjectDirectory from a directory containing multiple repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory(working_dir='/path/to/dir/', ignore=None, verbose=True)

The `ignore` parameter can be a list of directories to exclude. This method uses `os.walk` to search for `.git` directories recursively.

To check which repositories are included:

.. code-block:: python

    print(project._repo_name())

Explicit Local Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ProjectDirectory from a list of local repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory(
        working_dir=['/path/to/repo1/', '/path/to/repo2/'],
        ignore=None,
        verbose=True
    )

Each directory must contain a `.git` directory. Subdirectories are not searched.

Explicit Remote Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ProjectDirectory from remote repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory(
        working_dir=['git://github.com/user/repo.git'],
        ignore=None,
        verbose=True
    )

You can mix local and remote repositories. Remote repositories are cloned into temporary directories.

Common Operations
---------------

Here are some common operations you can perform with a ProjectDirectory object:

.. code-block:: python

    # Get general project information
    info_df = project.general_information()
    
    # Calculate bus factor
    bus_factor = project.bus_factor()
    
    # Get file change rates
    changes_df = project.file_change_rates()
    
    # Generate punchcard data
    punchcard_df = project.punchcard()

API Reference
------------

.. automodule:: gitpandas.project
   :members:
   :undoc-members:
   :show-inheritance:
   :inherited-members: