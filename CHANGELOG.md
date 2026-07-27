Unreleased
==========

## Bug Fixes

### File Ownership
 * **FIXED**: `Repository.file_owner()` always read the commit's *committer* regardless of the `committer` flag, so `committer=False` returned the top committer under a column labelled `author`. It now selects the identity to match the flag, matching `Repository.blame()`. This also fixes `Repository.file_detail(committer=False)` and `ProjectDirectory.file_detail(committer=False)`, whose `file_owner` column reported the committer on rebased, cherry-picked, squash-merged, or web-UI-committed history.

### MCP Server Serialization
 * **FIXED**: `serialize_pandas_object()` dropped every non-datetime DataFrame index during `orient="records"` conversion, so index-carried identity disappeared from tool results (e.g. a blame-shaped frame serialized as `[{"loc": 7}]` without the `committer`/`author` it belongs to). Named index levels, including `MultiIndex` levels such as `file` and `(tag_date, commit_date)`, are now materialized as columns.
 * **FIXED**: Serialization mutated the DataFrame it was given — rewriting a `DatetimeIndex` into formatted strings and replacing datetime columns in place — which corrupted shared cached frames. Serialization now works on a copy and leaves its input untouched.
 * **FIXED**: Serializing a frame that keeps a column and index level of the same name (e.g. `file_change_history()`'s `date`) raised `ValueError: cannot insert date, already exists`.

### GitHub Profile Discovery
 * **FIXED**: `GitHubProfile` now follows GitHub API pagination links and requests up to 100 repositories per page. If any page fails, discovery returns an empty profile instead of silently analyzing partial results.

### Hours Estimation
 * **FIXED**: `Repository.hours_estimate()` now includes the first-commit allowance, increasing estimates by `single_commit_hours` per contributor and giving single-commit contributors a non-zero estimate.

### Project Blame Aggregation
 * **FIXED**: `ProjectDirectory.blame()` now preserves committer/author and file grouping keys when combining multiple repositories. Contributors are aggregated by name across repositories, `blame(by="file")` no longer raises `KeyError`, and project-level bus factors are calculated from contributors instead of row numbers.

### Project File Blame Aggregation
 * **FIXED**: `ProjectDirectory.blame(by="file")` grouped on `(committer/author, file)` only. Because `file` is a repository-relative path, files sharing a path across repositories (`README.md`, `setup.py`, `__init__.py`, ...) were silently summed into a single row reporting a line count that matched neither repository, and the originating repository was discarded entirely. The repository is now part of the grouping, so the result is indexed by `(committer/author, file, repository)` — matching the identification that `file_detail()`, `file_change_rates()`, and `bus_factor(by="file")` already carry. `blame(by="repository")` is unchanged; aggregating a contributor across repositories there remains correct.

**Note**: This adds an index level to `blame(by="file")` output. Code that indexes that result by `(committer, file)` needs updating.

### Cache Correctness
 * **FIXED**: `@multicache` built cache keys from `kwargs` only, so any argument passed *positionally* was invisible to the key and collapsed to `None`. Keys are now resolved against the decorated method's signature (`inspect.signature().bind()` + `apply_defaults()`), so positional and keyword calls key identically. This fixes:
   - `Repository(working_dir=<master-only repo>, cache_backend=...)` raising `ValueError: Could not detect default branch` — the internal `has_branch("main")` / `has_branch("master")` probes shared one key.
   - `file_detail()` reporting the first file's owner for every file — the internal `file_owner(rev, file_path, ...)` calls shared one key.
 * **FIXED**: `blame()`'s `key_list` misspelled `ignore_globs` as `ignore_blobs`, so `ignore_globs` never contributed to the cache key and all variants collided. `blame(ignore_globs=[...])` and `bus_factor(ignore_globs=[...])` now return the same results with and without a cache backend.
 * **FIXED**: `skip_broken` was missing from the cache key of `file_change_history`, `file_change_rates`, `revs`, `cumulative_blame`, `parallel_cumulative_blame`, and `tags`, so `skip_broken=True` and `skip_broken=False` shared an entry.
 * **FIXED**: Cache key parts are now joined with `||` instead of `_`, which could occur inside the values themselves (e.g. `get_file_content(path="docs", rev="release_2")` collided with `path="docs_release", rev="2"`).
 * **NEW**: `@multicache` validates `key_list` against the decorated method's signature at decoration time and raises `ValueError` on a name that isn't a parameter, turning the typo class of bug into a loud failure.

**Note**: The cache key format has changed. Stale `EphemeralCache`/`DiskCache` entries simply miss and recompute, but `RedisDFCache` users sharing a cache across versions should flush it (or use a new key prefix) to avoid retaining entries under the old format.

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
