.. git-pandas documentation master file, created by
   sphinx-quickstart on Sun Nov  8 21:21:04 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Git-Pandas
==========

v2.0.0

A simple set of wrappers around gitpython for creating pandas dataframes out of git data. The project is centered around
two primary objects:

 * Repository
 * ProjectDirectory

A Repository object contains a single git repo, and is used to interact with it.  A ProjectDirectory references a directory
in your filesystem which may have in it multiple git repositories. The subdirectories are all walked to find any child
repos, and any analysis is aggregated up from all of those into a single output (pandas dataframe).

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


Detailed Documentation
======================

Currently, the two main sources of documentation are the repository and project pages, which have the Sphinx docs from
those two classes, as well as instructions on how to create the objects.  For detailed examples, check out the use-cases
page.

Contents:

.. toctree::
   :maxdepth: 2

   repository
   project
   usecases
   contributors


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

