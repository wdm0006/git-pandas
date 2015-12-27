Project Directory
=================

The ProjectDirectory object represents a collection of git repositories (perhaps in a single directory).  It can be
created in 3 main ways:

 * By specifying the directory in which the repositories live locally
 * By explicitly passing a list of local directories for each git repository
 * By explicitly passing remote git repositories to be cloned locally in temporary directories.

Once constructed, all work out equally, and as long as all are specified explicitly, you can even mix remote and local
repositories.

Using each method:

Directory Of Repositories
-------------------------

To create a ProjectDirectory object from a directory that contains multiple repositories simply use:

.. code-block:: python

   from gitpandas import ProjectDirectory
   pd = ProjectDirectory(working_dir='/path/to/dir/', ignore=None, verbose=True)


Where ignore can be a list of directories to explicitly ignore. This method uses os.walk to search the
passed directory for any .git directories, so even if a repository is many directories deep below the passed
working dir, it will be included.  To check what repositories are included in your object:

.. code-block:: python

   print(pd._repo_name())


Explicit Local
--------------

Explicit local directories are passed by using a list rather than a string for working dir:

.. code-block:: python

   from gitpandas import ProjectDirectory
   pd = ProjectDirectory(working_dir=['/path/to/repo1/', '/path/to/repo2/'], ignore=None, verbose=True)

In this case, the subdirectories of the directories passed are not searched, so every directory passed
must have a .git directory in it.

Explicit Remote
---------------

Explicit local directories are passed by using a list rather than a string for working dir:

.. code-block:: python

   from gitpandas import ProjectDirectory
   pd = ProjectDirectory(working_dir=['git://github.com/user/repo.git'], ignore=None, verbose=True)

As mentioned, you can mix explicit remote and explicit local repositories, the remote repos will be cloned
into temporary directories and treated as local ones under the hood. Because of this, for large repos, it
can be relatively slow to create ProjectDirectory objects with many explicit remote repositories.


Detailed API Documentation
--------------------------

.. automodule:: gitpandas.project
   :members: