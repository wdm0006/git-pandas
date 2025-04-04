.. git-pandas documentation master file, created by
   sphinx-quickstart on Sun Nov  8 21:21:04 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Git-Pandas Documentation
===================================

Git-Pandas is a powerful Python library that transforms Git repository data into pandas DataFrames, making it easy to analyze and visualize your codebase's history, contributors, and development patterns.

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
    repo = Repository('/path/to/repo')
    commits_df = repo.commit_history()
    blame_df = repo.blame()

Analyze multiple repositories:

.. code-block:: python

    from gitpandas import ProjectDirectory
    project = ProjectDirectory('/path/to/project')
    project_info = project.general_information()

Key Features
------------

* **Repository Analysis**: Extract commit history, file changes, and blame information
* **Project Insights**: Calculate bus factor, development time, and contributor metrics
* **GitHub Integration**: Analyze GitHub profiles and repository metrics
* **Visualization Tools**: Built-in plotting utilities for common Git analytics
* **Performance Optimization**: Optional caching support for memory-intensive operations

Core Components
---------------

The library is built around two main components:

* **Repository**: A wrapper around a single Git repository
* **ProjectDirectory**: A collection of Git repositories for aggregate analysis

For detailed information about these components, see the :doc:`repository` and :doc:`project` documentation.

Documentation
-------------

Currently, the two main sources of documentation are the repository and project pages, which have the Sphinx docs from
those two classes, as well as instructions on how to create the objects. For detailed examples, check out the use-cases
page.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   repository
   project
   cache
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

