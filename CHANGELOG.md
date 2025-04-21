v2.3.0
======

 * Updated coverage file parsing to use coverage.py API instead of direct file parsing
 * Added coverage>=5.0.0 as a core dependency
 * Added a basic MCP server
 * Added methods to `Repository` for getting files in repo, getting content of a file, and getting diffs of a revision


v2.2.1
======

 * Docs CI bugfix

v2.2.0
======

 * Support for default branch setting instead of assuming master, will infer if not passed
 * Better handling of ignore repos in project directory setup
 * Added a branch exists helper in repository 
 * Docs corrections

v2.1.0
======

 * Imrpoved test suite
 * Many bugfixes
 * Updates for pandas v2

v2.0.0
======

 * Fully transitioned to ignore_globs and include_globs style syntax
 * Parallelized cumulative blame support with joblib threading backend
 * Added threading parallelism to many project directory functions.
 * Added a chaching module for optional redis or memory backed caching of certain resultsets
 
v1.2.0
======

 * Added ignore_globs option alongside all methods with ignore_dir and extensions, will be the only method for filtering files in v2.0.0
 
v1.1.0
======

 * _repo_name changed to repo_name in project directories (old method left with deprecation warning)
 * repo_name property added to repositories
 
v1.0.3
======

 * Support for estimating time spent developing on projects.
 
v1.0.2
======

 * bugfix in ignore_dir option for root level directories

v1.0.1
======

 * file details function
 
v1.0.0
======

 * Stable API
 * Punchcard dataframe added
 * Plotting helpers added to library under utilities module
 * Added github.com profile object

v0.0.6
======

 * Added file owner utility
 * Added lifelines example
 * Added rev to file change history table
 * Added file-wise blame using by='file' parameter
 * Bus Factor returns a dataframe
 * Now supporting python 2.7+ and 3.3+

v0.0.5
======

 * Added file change rates table with risk metrics
 * Added basic functionality with coverage files
 * Added limited time window based dataset functionality
 * Expanded docs
 
v0.0.4
======

 * Added cumulative blame and revision history
 
v0.0.3
======

 * Added approximate bus factor analysis

v0.0.2
======

 * Added blame

v0.0.1
======

 * Initial release, basic interface to commit history and descriptors