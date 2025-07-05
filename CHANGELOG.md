v2.5.0
======

## New Features

### Remote Operations & Cache Warming
 * **NEW**: `Repository.safe_fetch_remote()` - Safely fetch changes from remote repositories without modifying working directory
   - Read-only operation with comprehensive error handling
   - Support for dry-run preview and remote validation
   - Configurable remote names and pruning options
 * **NEW**: `Repository.warm_cache()` - Pre-populate repository cache for improved performance  
   - Configurable method selection with intelligent parameter handling
   - Performance metrics and cache entry tracking
   - Significant performance improvements (1.5-10x speedup demonstrated)
 * **NEW**: `ProjectDirectory.bulk_fetch_and_warm()` - Efficiently process multiple repositories
   - Parallel processing support when joblib is available
   - Error isolation (failures in one repo don't affect others)
   - Comprehensive summary statistics and progress tracking

### Enhanced Caching System
 * **NEW**: `CacheEntry` class with metadata tracking (timestamps, age calculation)
 * **ENHANCED**: Thread-safe cache operations with proper locking mechanisms  
 * **ENHANCED**: Cache key consistency improvements using `||` delimiter format
 * **ENHANCED**: Cache timestamp and metadata access methods (`get_cache_info()`, `list_cached_keys()`)

### Documentation & Examples
 * **NEW**: Comprehensive remote operations documentation (`docs/source/remote_operations.rst`)
 * **NEW**: Cache warming and remote fetch example (`examples/remote_fetch_and_cache_warming.py`)
 * **NEW**: Cache timestamp usage example (`examples/cache_timestamps.py`)
 * **NEW**: Release analytics example (`examples/release_analytics.py`)

## Testing & Quality
 * **NEW**: 38 comprehensive tests for remote operations and cache warming
 * **NEW**: Thread safety tests for cache operations
 * **NEW**: Edge case and error handling test coverage
 * **IMPROVED**: Overall test coverage and reliability
 * **FIXED**: Various minor bugs and future warnings

## Backward Compatibility
 * All new features are fully backward compatible
 * No breaking changes to existing APIs
 * Existing cache backends work seamlessly with new features

v2.4.0
======

 * Significant caching bugfixes and updates
 * Added a DiskCache that persists across runs
 * Added release analytics 

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