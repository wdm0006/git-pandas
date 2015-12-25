.. git-pandas documentation master file, created by
   sphinx-quickstart on Sun Nov  8 21:21:04 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Git-Pandas
==========

v0.0.4

A simple set of wrappers around gitpython for creating pandas dataframes out of git data. The project is centered around
two primary objects:

 * Repository
 * ProjectDirectory

A Repository object contains a single git repo, and is used to interact with it.  A ProjectDirectory references a directory
in your filesystem which may have in it multiple git repositories. The subdirectories are all walked to find any child
repos, and any analysis is aggregated up from all of those into a single output (pandas dataframe).


This is a pre-v1.0.0 project, so the interfaces and functionality may change.

Current functionality includes:

 * Commit history with extension and directory filtering
 * Edited files history with extension and directory filtering
 * Blame with extension and directory filtering
 * Branches
 * Tags
 * ProjectDirectory-level general information table
 * Approximate bus factor
 * Cumulative Blame as a time series

Please see examples for more detailed usage. The image above is generated using the repository object's cumulative blame
function on stravalib.


Installation
------------

To install use:

    pip install git-pandas

Contributing
------------

We are looking for contributors, so if you are interested, please review our contributor guidelines in CONTRIBUTING.md,
which includes some proposed starter issues, or if you have an idea of your own, send us a pull request.

License
-------

This is BSD licensed (see LICENSE.md)

Contents:

.. toctree::
   :maxdepth: 2

   repository
   project
   attributes
   commit_history
   bus_factor
   cumulative_blame
   coverage
   file_change_rates


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

