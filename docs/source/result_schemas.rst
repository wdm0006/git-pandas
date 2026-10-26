.. This page is generated: python docs/source/schema_contracts.py > docs/source/result_schemas.rst

Result schemas
==============

Exact column order, index levels and dtype kinds of populated results, pinned by
``tests/test_schema_contracts.py``. Kinds are ``str``, ``int``, ``float``, ``bool`` and ``datetime``
(UTC); they hold on pandas 2 and 3. Shapes differ between ``Repository`` and ``ProjectDirectory`` and
between populated and empty results; both are listed as they are today.

Repository.commit_history()
---------------------------

Index: ``date`` (datetime)

UTC ``date`` index; includes a ``branch`` column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``author``
     - str
   * - ``committer``
     - str
   * - ``message``
     - str
   * - ``commit_sha``
     - str
   * - ``lines``
     - int
   * - ``insertions``
     - int
   * - ``deletions``
     - int
   * - ``net``
     - int
   * - ``branch``
     - str
   * - ``repository``
     - str

ProjectDirectory.commit_history()
---------------------------------

Index: ``(unnamed integer index)`` (int)

``date`` is a column; the integer index restarts at 0 for each repository; no ``branch`` column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``repository``
     - str
   * - ``author``
     - str
   * - ``committer``
     - str
   * - ``date``
     - datetime
   * - ``message``
     - str
   * - ``commit_sha``
     - str
   * - ``lines``
     - int
   * - ``insertions``
     - int
   * - ``deletions``
     - int
   * - ``net``
     - int

Repository.file_change_history()
--------------------------------

Index: ``date`` (datetime)

``date`` is both the index and a column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``filename``
     - str
   * - ``insertions``
     - int
   * - ``deletions``
     - int
   * - ``lines``
     - int
   * - ``message``
     - str
   * - ``committer``
     - str
   * - ``author``
     - str
   * - ``date``
     - datetime
   * - ``repository``
     - str

ProjectDirectory.file_change_history()
--------------------------------------

Index: ``(unnamed integer index)`` (int)

No ``lines`` column; ``date`` is a column on a default integer index.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``repository``
     - str
   * - ``date``
     - datetime
   * - ``author``
     - str
   * - ``committer``
     - str
   * - ``message``
     - str
   * - ``filename``
     - str
   * - ``insertions``
     - int
   * - ``deletions``
     - int

Repository.revs()
-----------------

Index: ``(unnamed integer index)`` (int)

``date`` is epoch seconds stored as an integer column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``date``
     - int
   * - ``rev``
     - str
   * - ``repository``
     - str

ProjectDirectory.revs()
-----------------------

Index: ``(unnamed integer index)`` (int)

``date`` is epoch seconds stored as a float column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``repository``
     - str
   * - ``rev``
     - str
   * - ``date``
     - float

Repository.blame(by='repository')
---------------------------------

Index: ``committer`` (str)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``loc``
     - int
   * - ``repository``
     - str

ProjectDirectory.blame(by='repository')
---------------------------------------

Index: ``committer`` (str)

No ``repository`` column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``loc``
     - int

Repository.blame(by='file')
---------------------------

Index: ``committer`` (str), ``file`` (str)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``loc``
     - int
   * - ``repository``
     - str

ProjectDirectory.blame(by='file')
---------------------------------

Index: ``committer`` (str), ``file`` (str), ``repository`` (str)

``repository`` is an index level, not a column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``loc``
     - int

Repository.tags()
-----------------

Index: ``tag_date`` (datetime), ``commit_date`` (datetime)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``tag``
     - str
   * - ``annotated``
     - bool
   * - ``annotation``
     - nullable_str
   * - ``tag_sha``
     - nullable_str
   * - ``commit_sha``
     - str
   * - ``repository``
     - str

ProjectDirectory.tags()
-----------------------

Index: ``tag_date`` (datetime), ``commit_date`` (datetime)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``tag``
     - str
   * - ``annotated``
     - bool
   * - ``annotation``
     - nullable_str
   * - ``tag_sha``
     - nullable_str
   * - ``commit_sha``
     - str
   * - ``repository``
     - str

Repository.commits_in_tags()
----------------------------

Index: ``tag_date`` (datetime), ``commit_date`` (datetime)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``commit_sha``
     - str
   * - ``tag``
     - str
   * - ``repository``
     - str

ProjectDirectory.commits_in_tags()
----------------------------------

Index: ``tag_date`` (datetime), ``commit_date`` (datetime)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``commit_sha``
     - str
   * - ``tag``
     - str
   * - ``repository``
     - str

Repository.punchcard()
----------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``hour_of_day``
     - int
   * - ``day_of_week``
     - int
   * - ``lines``
     - int
   * - ``insertions``
     - int
   * - ``deletions``
     - int
   * - ``net``
     - int

ProjectDirectory.punchcard()
----------------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``hour_of_day``
     - int
   * - ``day_of_week``
     - int
   * - ``lines``
     - int
   * - ``insertions``
     - int
   * - ``deletions``
     - int
   * - ``net``
     - int

Repository.hours_estimate()
---------------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``committer``
     - str
   * - ``hours``
     - float
   * - ``repository``
     - str

ProjectDirectory.hours_estimate()
---------------------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``committer``
     - str
   * - ``hours``
     - float
   * - ``repository``
     - str

Repository.file_detail()
------------------------

Index: ``(unnamed integer index)`` (int)

``file`` is a column.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``file``
     - str
   * - ``loc``
     - int
   * - ``file_owner``
     - str
   * - ``ext``
     - str
   * - ``last_edit_date``
     - datetime
   * - ``repository``
     - str

ProjectDirectory.file_detail()
------------------------------

Index: ``file`` (str), ``repository`` (str)

``(file, repository)`` index; a stray ``index`` column of row numbers leaks through.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``index``
     - int
   * - ``loc``
     - int
   * - ``file_owner``
     - str
   * - ``ext``
     - str
   * - ``last_edit_date``
     - datetime

Repository.bus_factor(by='repository')
--------------------------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``repository``
     - str
   * - ``bus factor``
     - int

ProjectDirectory.bus_factor(by='projectd')
------------------------------------------

Index: ``(unnamed integer index)`` (int)

Default grouping; the first column is named ``projectd``.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``projectd``
     - str
   * - ``bus factor``
     - int

Repository.bus_factor(by='file')
--------------------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``file``
     - str
   * - ``bus factor``
     - int
   * - ``repository``
     - str

ProjectDirectory.bus_factor(by='file')
--------------------------------------

Index: ``(unnamed integer index)`` (int)

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``file``
     - str
   * - ``bus factor``
     - int
   * - ``repository``
     - str

ProjectDirectory.bus_factor(by='repository')
--------------------------------------------

Index: ``(unnamed integer index)`` (int)

``bus factor`` has object dtype.

.. list-table::
   :header-rows: 1

   * - Column
     - Kind
   * - ``repository``
     - str
   * - ``bus factor``
     - object

Empty results
-------------

Where the empty frame differs from the populated one:

.. list-table::
   :header-rows: 1

   * - Method
     - Columns
     - Index
   * - Repository.file_change_history
     - ``filename``, ``insertions``, ``deletions``, ``lines``, ``message``, ``committer``, ``author``
     - (unnamed integer index)
   * - Repository.revs
     - ``date``, ``rev``
     - (unnamed integer index)
   * - Repository.tags
     - ``tag_date``, ``commit_date``, ``tag``, ``annotated``, ``annotation``, ``tag_sha``, ``commit_sha``, ``repository``
     - (unnamed integer index)
   * - Repository.commits_in_tags
     - ``commit_sha``, ``tag``, ``tag_date``, ``commit_date``
     - (unnamed integer index)
   * - ProjectDirectory.revs
     - ``repository``, ``rev``
     - (unnamed integer index)
   * - ProjectDirectory.tags
     - (none)
     - (unnamed integer index)
   * - ProjectDirectory.commits_in_tags
     - ``commit_sha``, ``tag``, ``tag_date``, ``commit_date``
     - (unnamed integer index)
