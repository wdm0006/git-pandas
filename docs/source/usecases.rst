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


Example Setup
^^^^^^^^^^^^^

The following examples exhibit the basic functionality of the ProjectDirectory and Repository objects, for gathering
the basic attributes of them. In this section, we use the example: attributes.py, which can be found in the examples
directory. For more detailed information, please check the API reference in previous sections.

For the following examples, we will use 2 objects, defined by:

.. code-block:: python

    from gitpandas import Repository, ProjectDirectory
    p = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git', 'git://github.com/CamDavidsonPilon/lifelines.git'])
    r = Repository(working_dir='git://github.com/wdm0006/git-pandas.git')


Repository Name
^^^^^^^^^^^^^^^

To access the approximate repository name of each:

.. code-block:: python

    print('Project Directory Names:')
    print(p._repo_name())
    print('\nRepository Name:')
    print(r._repo_name())

Which will yield:

.. code-block:: none

    Project Directory Names:
       repository
    0  git-pandas
    1   lifelines

    Repository Name:
    git-pandas

Is Bare
^^^^^^^

To find out if the repositories are bare:

.. code-block:: python

    print('Project Directory Is Bare:')
    print(p.is_bare())
    print('\nRepository Is Bare:')
    print(r.is_bare())

Which will yield:

.. code-block:: none

    Project Directory Is Bare:
       repository is_bare
    0  git-pandas   False
    1   lifelines   False

    Repository Is Bare:
    False

Tags
^^^^

To access the tags of each:

.. code-block:: python

    print('Project Directory Tags:')
    print(p.tags())
    print('\nRepository Tags:')
    print(r.tags())

Which will yield:

.. code-block:: none

    Project Tags:
        repository       tag
    0   git-pandas     0.0.1
    1   git-pandas     0.0.2
    2   git-pandas     0.0.3
    3   git-pandas     0.0.4
    4   git-pandas     0.0.5
    0    lifelines     0.4.3
    1    lifelines     0.6.0
    2    lifelines    ignore
    3    lifelines      v0.4
    4    lifelines    v0.4.1
    5    lifelines    v0.4.2
    6    lifelines    v0.4.4
    7    lifelines  v0.4.4.1
    8    lifelines    v0.5.0
    9    lifelines    v0.5.1
    10   lifelines    v0.6.0
    11   lifelines    v0.7.0
    12   lifelines    v0.8.0

    Repository Tags:
         tag  repository
    0  0.0.1  git-pandas
    1  0.0.2  git-pandas
    2  0.0.3  git-pandas
    3  0.0.4  git-pandas
    4  0.0.5  git-pandas

Branches
^^^^^^^^

To access the branches of each:

.. code-block:: python

    print('Project Directory Branches:')
    print(p.branches())
    print('\nRepository Branches:')
    print(r.branches())

Which will yield:

.. code-block:: none

    Project Branches:
        branch   local   repository
    0   master    True   git-pandas
    1   master    False  git-pandas
    2   gh-pages  False  git-pandas
    0   master    True   lifelines
    1   0.6.0     False  lifelines
    ...

    Repository Branches:
         branch  local  repository
    0  gh-pages   True  git-pandas
    1    master   True  git-pandas
    2    master  False  git-pandas
    3  gh-pages  False  git-pandas

Revisions
^^^^^^^^^

To access the revisions of each:

.. code-block:: python

    print('Project Directory Revisions:')
    print(p.revs())
    print('\nRepository Revisions:')
    print(r.revs())

Which will yield:

.. code-block:: none

    Project Directory Revisions:
               date  repository                                       rev
    0    1451844740  git-pandas  5cbf630d723f9ebdd0e164eb58a6fe952f1cb92c
    1    1451843631  git-pandas  0b72b01b2b4a0cf673f457e016cdcdde8fe82f15
    2    1451842103  git-pandas  4376d9451d1ff32089d0dd1bffa3de56fe35604d
    3    1451842081  git-pandas  ebfdadc6d09d613b948dadef986bd9cbea4240a2
    ...
    0    1450720064   lifelines  e689d8d910b65cd2c2188c74e33ef2f722d361a4
    1    1450719167   lifelines  773670a6261326d96556816f48e159cbceaeeb2d
    2    1450718313   lifelines  d42a010cfa368975c0beaa251db8db2cacdf9be1
    3    1450718269   lifelines  a1543344f91918e2f3456cf15d1895ac6448f8a5
    ...

    Repository Revisions:
              date                                       rev
    0   1451844740  5cbf630d723f9ebdd0e164eb58a6fe952f1cb92c
    1   1451843631  0b72b01b2b4a0cf673f457e016cdcdde8fe82f15
    2   1451842103  4376d9451d1ff32089d0dd1bffa3de56fe35604d
    3   1451842081  ebfdadc6d09d613b948dadef986bd9cbea4240a2
    ...

Blame
^^^^^

To access the current blame of each:

.. code-block:: python

    print('Project Directory Blame:')
    print(p.blame(include_globs=['*.py']))
    print('\nRepository Blame:')
    print(r.blame(include_globs=['*.py']))

Which will yield:

.. code-block:: none

    Project Directory Blame:
                             loc
    Cameron Davidson-Pilon  5537
    Will McGinnis           1789
    Jonas Kalderstam         434
    Will Mcginnis            316
    CamDavidsonPilon         236
    Ben Kuhn                  94
    Nick Evans                20
    Andrew Gartland           14
    Kyle                       9
    xantares                   6
    Niels Bantilan             5
    Ben Rifkind                1
    Nick Furlotte              1

    Repository Blame:
                    loc
    committer
    Will McGinnis  1750
    Will Mcginnis   316

Commit History
--------------

One of the simplest datasets to be pulled from a repository or collection of repositories is the
commit history.  This is done via:

 * commit history
 * file change history

Example Setup
^^^^^^^^^^^^^

In this section, we use the example: commit_history.py, which can be found in the examples directory. For more detailed
information, please check the API reference in previous sections.

For the following examples, we will use 2 objects, defined by:

.. code-block:: python

    from gitpandas import Repository, ProjectDirectory
    p = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git', 'git://github.com/CamDavidsonPilon/lifelines.git'])
    r = Repository(working_dir='git://github.com/wdm0006/git-pandas.git')

Commit History
^^^^^^^^^^^^^^

TODO

File Change History
^^^^^^^^^^^^^^^^^^^

TODO

Bus Factor
----------

One major block of functionality is to do bus factor analysis on repos and collections of repos.
This includes at the highest level, and in hierarchical terms (in the future). This functionality is
accessed by:

 * bus factor

Example Setup
^^^^^^^^^^^^^

In this section, we use the example: bus_factor.py, which can be found in the examples directory. For more detailed
information, please check the API reference in previous sections.

For the following examples, we will use 2 objects, defined by:

.. code-block:: python

    from gitpandas import Repository, ProjectDirectory
    p = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git', 'git://github.com/CamDavidsonPilon/lifelines.git'])
    r = Repository(working_dir='git://github.com/wdm0006/git-pandas.git')


Bus Factor
^^^^^^^^^^

TODO

Cumulative Blame
----------------

Another major block of functionality in git-pandas is the cumulative blame interface.  This allows you to
track and visualize the share of a project borne by individual committers or repositories over time.

It is accessed by:

 * cumulative_blame

Example Setup
^^^^^^^^^^^^^

In this section, we use the example: cumulative_blame.py, which can be found in the examples directory. For more detailed
information, please check the API reference in previous sections.

For the following examples, we will use 2 objects, defined by:

.. code-block:: python

    from gitpandas import Repository, ProjectDirectory
    p = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git', 'git://github.com/CamDavidsonPilon/lifelines.git'])
    r = Repository(working_dir='git://github.com/wdm0006/git-pandas.git')


Cumulative Blame
^^^^^^^^^^^^^^^^

TODO

Coverage
--------

If a .coverage file is available, we have experimental support for integrating that data in with the git data.
This functionality is accessed by:

 * has_coverage
 * coverage

Example Setup
^^^^^^^^^^^^^

In this section, we use the example: coverage_data.py, which can be found in the examples directory. For more detailed
information, please check the API reference in previous sections.

For the following examples, we will use 2 objects, defined by:

.. code-block:: python

    from gitpandas import Repository, ProjectDirectory
    p = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git', 'git://github.com/CamDavidsonPilon/lifelines.git'])
    r = Repository(working_dir='git://github.com/wdm0006/git-pandas.git')

Has Coverage
^^^^^^^^^^^^

TODO

Coverage
^^^^^^^^

TODO

File Change Rates
-----------------

File change rate, or risk, is a specialized dataframe aimed at identifying files which are likely to have bugs in them.
If coverage data is available, that can be included in this table.

 * file_change_rates

Example Setup
^^^^^^^^^^^^^

In this section, we use the example: file_change_rates.py, which can be found in the examples directory. For more detailed
information, please check the API reference in previous sections.

For the following examples, we will use 2 objects, defined by:

.. code-block:: python

    from gitpandas import Repository, ProjectDirectory
    p = ProjectDirectory(working_dir=['git://github.com/wdm0006/git-pandas.git', 'git://github.com/CamDavidsonPilon/lifelines.git'])
    r = Repository(working_dir='git://github.com/wdm0006/git-pandas.git')

File Change Rates
^^^^^^^^^^^^^^^^^

TODO
