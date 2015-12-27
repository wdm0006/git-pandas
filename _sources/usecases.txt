Use Cases
=========

Git-Pandas aims to at the most basic level provide a pandas-based interface to the data contained in git
repositories. Beyond that, there are many specific use cases pertaining to the management and analysis of
multi-repo projects and organizations that git-pandas can help with.  Here we will outline some of the various
use-cases that git-pandas is good at, and how you can use it in your projects or organizations.

Attributes
----------

At the most basic level, git-pandas allows a panda's based interaction with the basic attributes of a repo.
 This includes:

 * estimated repository name
 * tags
 * branches
 * revs
 * blame
 * is_bare

For detailed usage and examples:

Repository Name
^^^^^^^^^^^^^^^


Tags
^^^^


Branches
^^^^^^^^


Revisions
^^^^^^^^^



Blame
^^^^^



Is Bare
^^^^^^^



Commit History
--------------

One of the simplest datasets to be pulled from a repository or collection of repositories is the
commit history.  This is done via:

 * commit history
 * file change history


Commit History
^^^^^^^^^^^^^^



File Change History
^^^^^^^^^^^^^^^^^^^



Bus Factor
----------

One major block of functionality is to do bus factor analysis on repos and collections of repos.
This includes at the highest level, and in hierarchical terms (in the future). This functionality is
accessed by:

 * bus factor


Bus Factor
^^^^^^^^^^



Cumulative Blame
----------------

Another major block of functionality in git-pandas is the cumulative blame interface.  This allows you to
track and visualize the share of a project borne by individual committers or repositories over time.

It is accessed by:

 * cumulative_blame


Cumulative Blame
^^^^^^^^^^^^^^^^


Coverage
--------

If a .coverage file is available, we have experimental support for integrating that data in with the git data.
This functionality is accessed by:

 * has_coverage
 * coverage


Has Coverage
^^^^^^^^^^^^



Coverage
^^^^^^^^


File Change Rates
-----------------

File change rate, or risk, is a specialized dataframe aimed at identifying files which are likely to have bugs in them.
If coverage data is available, that can be included in this table.

 * file_change_rates

File Change Rates
^^^^^^^^^^^^^^^^^

