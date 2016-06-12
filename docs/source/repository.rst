Repository
==========

The repository object represents one single git repository, can be created in two main ways:

 * By explicitly passing the directory of a local git repository
 * By explicitly passing a remote git repository to be cloned locally into a temporary directory.

Using each method:

Explicit Local
--------------

Explicit local directories are passed by using a string for working dir:

.. code-block:: python

   from gitpandas import Repository
   pd = Repository(working_dir='/path/to/repo1/', verbose=True)

The subdirectories of the directory passed are not searched, so it must have a .git directory in it.

Explicit Remote
---------------

Explicit remote directories are passed by using simple git notation:

.. code-block:: python

   from gitpandas import Repository
   pd = Repository(working_dir='git://github.com/user/repo.git', verbose=True)

The repository will be cloned locally into a temporary directory, which can be somewhat slow for large repos.

Detailed API Documentation
--------------------------
.. automodule:: gitpandas.repository
   :members: