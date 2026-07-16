import os
import subprocess
import tempfile

import pandas as pd
import pytest

from gitpandas import Repository
from gitpandas.cache import DiskCache, EphemeralCache, multicache


class RepositoryMock:
    """Mock Repository class to test cache key generation consistency"""

    def __init__(self, working_dir="/mock/repo/path", cache_backend=None):
        self.working_dir = working_dir
        self.repo_name = working_dir  # Simulate how Repository sets repo_name
        self.cache_backend = cache_backend
        self.execution_count = 0

    @multicache(key_prefix="list_files", key_list=["directory", "filter_regex"])
    def list_files(self, directory=None, filter_regex=None, force_refresh=False):
        """Mock list_files method that demonstrates the caching behavior"""
        self.execution_count += 1
        return pd.DataFrame(
            {
                "file": [f"file{i}.txt" for i in range(3)],
                "directory": [directory] * 3,
                "call_number": [self.execution_count] * 3,
            }
        )

    @multicache(key_prefix="complex_method", key_list=["param1", "param2", "param3"])
    def complex_method(self, param1=None, param2=None, param3=None, force_refresh=False):
        """Method with multiple parameters to test key generation with many parameters"""
        self.execution_count += 1
        return pd.DataFrame(
            {
                "result": [f"result{i}" for i in range(2)],
                "param_values": [f"{param1}_{param2}_{param3}"] * 2,
                "call_number": [self.execution_count] * 2,
            }
        )


class TestCacheKeyConsistency:
    """Tests specifically focused on the cache key consistency issue"""

    @pytest.fixture
    def temp_cache_path(self):
        """Create temporary file path for cache"""
        fd, path = tempfile.mkstemp(suffix=".gz")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def test_consistent_cache_keys(self, temp_cache_path):
        """Test that cache keys are consistent between method calls"""
        cache = DiskCache(filepath=temp_cache_path)
        repo = RepositoryMock(cache_backend=cache)

        # Capture cache keys generated
        original_set = cache.set
        captured_keys = []

        def mock_set(k, v):
            captured_keys.append(k)
            return original_set(k, v)

        cache.set = mock_set

        # First call
        repo.list_files(directory="src", filter_regex="*.py")
        first_key = captured_keys[0]

        # Clear captured keys
        captured_keys.clear()

        # Second call with identical parameters
        repo.list_files(directory="src", filter_regex="*.py")

        # No key should be captured on second call (cache hit)
        assert len(captured_keys) == 0

        # Force refresh should use the same key
        repo.list_files(directory="src", filter_regex="*.py", force_refresh=True)
        assert len(captured_keys) == 1
        assert captured_keys[0] == first_key

    def test_fix_resolves_reported_issue(self, temp_cache_path):
        """Test specifically addressing the reported issue"""
        cache = DiskCache(filepath=temp_cache_path)
        repo = RepositoryMock(working_dir="/absolute/path/to/repo", cache_backend=cache)

        # First call
        result1 = repo.list_files()
        assert repo.execution_count == 1

        # Second call to the same method (should use cache)
        result2 = repo.list_files()
        assert repo.execution_count == 1  # Should NOT increment

        # Results should match
        pd.testing.assert_frame_equal(result1, result2)

    def test_varied_path_formats(self, temp_cache_path):
        """Test with different path formats to ensure key consistency"""
        cache = DiskCache(filepath=temp_cache_path)

        # Different repo path formats
        repo1 = RepositoryMock(working_dir="/path/to/repo", cache_backend=cache)
        repo2 = RepositoryMock(working_dir="/path/to/repo/", cache_backend=cache)  # Extra slash

        # Capture all keys set in the cache
        original_set = cache.set
        captured_keys = []

        def mock_set(k, v):
            captured_keys.append(k)
            return original_set(k, v)

        cache.set = mock_set

        # Call method on first repo
        repo1.list_files(directory="src")
        assert repo1.execution_count == 1
        key1 = captured_keys[0]

        # Clear keys
        captured_keys.clear()

        # Call on second repo with same parameters - should generate a different key
        # due to different repo_name ("/path/to/repo" vs "/path/to/repo/")
        repo2.list_files(directory="src")
        assert repo2.execution_count == 1  # Should increment for repo2
        key2 = captured_keys[0]

        # Keys should be different because repo_name is different
        assert key1 != key2
        assert "||/path/to/repo||" in key1
        assert "||/path/to/repo/||" in key2

    def test_complex_key_generation(self, temp_cache_path):
        """Test key generation with complex parameters"""
        cache = DiskCache(filepath=temp_cache_path)
        repo = RepositoryMock(cache_backend=cache)

        # Capture keys
        original_set = cache.set
        captured_keys = []

        def mock_set(k, v):
            captured_keys.append(k)
            return original_set(k, v)

        cache.set = mock_set

        # Call with complex parameters
        repo.complex_method(param1="value1", param2="value2", param3="value3")

        # Check key format
        key = captured_keys[0]
        assert key.startswith("complex_method||")
        assert key.endswith("||value1||value2||value3")

        # Call again with different order of parameters in the call
        # Python should normalize kwargs, so the key should be the same
        captured_keys.clear()
        repo.complex_method(param3="value3", param1="value1", param2="value2", force_refresh=True)

        # Key should be the same despite different parameter order
        assert captured_keys[0] == key


@pytest.fixture
def master_only_repo():
    """A two-file repo on a 'master' branch with known line counts and a single author.

    keep.py has 3 lines, vendor.js has 6 lines, so blame totals are exactly known.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull}

        def git(*cmd):
            subprocess.run(["git", "-C", tmpdir, *cmd], check=True, capture_output=True, env=env)

        subprocess.run(["git", "init", "-b", "master", tmpdir], check=True, capture_output=True, env=env)
        git("config", "user.name", "Test Author")
        git("config", "user.email", "test@example.com")

        with open(os.path.join(tmpdir, "keep.py"), "w") as f:
            f.write("a = 1\nb = 2\nc = 3\n")
        with open(os.path.join(tmpdir, "vendor.js"), "w") as f:
            f.write("\n".join(f"var v{i} = {i};" for i in range(6)) + "\n")

        git("add", "keep.py", "vendor.js")
        git("commit", "-m", "initial")

        yield tmpdir


class TestCachedResultsMatchUncached:
    """Attaching a cache backend must not change results — only speed."""

    def test_master_only_repo_constructs_with_cache_backend(self, master_only_repo):
        """has_branch('main')/has_branch('master') are called positionally in __init__.

        Keying on kwargs only collapses both to the same key, so the cached False from
        the 'main' probe answers the 'master' probe and default branch detection fails.
        """
        cached = Repository(working_dir=master_only_repo, cache_backend=EphemeralCache())
        assert cached.default_branch == "master"
        assert Repository(working_dir=master_only_repo).default_branch == "master"

    def test_has_branch_positional_and_keyword_share_one_key(self, master_only_repo):
        cache = EphemeralCache()
        repo = Repository(working_dir=master_only_repo, cache_backend=cache)

        assert repo.has_branch("master") is True
        assert repo.has_branch(branch="master") is True

        has_branch_keys = [k for k in cache._cache if k.startswith("has_branch||")]
        assert len(has_branch_keys) == 2  # one for the 'main' probe, one for 'master'
        assert sum(k.endswith("||master") for k in has_branch_keys) == 1

    def test_blame_ignore_globs_respected_with_cache(self, master_only_repo):
        uncached = Repository(working_dir=master_only_repo)
        cached = Repository(working_dir=master_only_repo, cache_backend=EphemeralCache())

        def loc(repo, **kwargs):
            return repo.blame(**kwargs)["loc"].sum()

        # Warm the cache with the unfiltered call first — the ignore_globs call must not hit it.
        assert loc(cached) == 9
        assert loc(uncached) == 9

        assert loc(cached, ignore_globs=["*.js"]) == 3
        assert loc(uncached, ignore_globs=["*.js"]) == 3

    def test_bus_factor_ignore_globs_respected_with_cache(self, master_only_repo):
        cached = Repository(working_dir=master_only_repo, cache_backend=EphemeralCache())
        uncached = Repository(working_dir=master_only_repo)

        cached.blame()  # poison-prone warm-up: caches the unfiltered blame first

        cached_bf = cached.bus_factor(ignore_globs=["*.js"])
        uncached_bf = uncached.bus_factor(ignore_globs=["*.js"])
        pd.testing.assert_frame_equal(cached_bf, uncached_bf)

    def test_file_owner_distinct_key_per_filename(self, master_only_repo):
        cache = EphemeralCache()
        repo = Repository(working_dir=master_only_repo, cache_backend=cache)

        repo.file_owner("HEAD", "keep.py", committer=True)
        repo.file_owner("HEAD", "vendor.js", committer=True)

        file_owner_keys = [k for k in cache._cache if k.startswith("file_owner||")]
        assert len(file_owner_keys) == 2

    def test_file_detail_owners_match_uncached(self, master_only_repo):
        cached = Repository(working_dir=master_only_repo, cache_backend=EphemeralCache())
        uncached = Repository(working_dir=master_only_repo)

        pd.testing.assert_frame_equal(cached.file_detail(), uncached.file_detail())

    def test_get_file_content_key_parts_do_not_collide(self, master_only_repo):
        """path='docs', rev='release_2' must not key the same as path='docs_release', rev='2'."""
        cache = EphemeralCache()
        repo = Repository(working_dir=master_only_repo, cache_backend=cache)

        repo.get_file_content(path="docs", rev="release_2")
        repo.get_file_content(path="docs_release", rev="2")

        assert len([k for k in cache._cache if k.startswith("get_file_content||")]) == 2


class TestKeyListValidation:
    """key_list must name real parameters, checked when the decorator is applied."""

    def test_unknown_key_list_entry_rejected_at_decoration_time(self):
        with pytest.raises(ValueError, match="do not exist"):

            class Bad:
                cache_backend = None
                repo_name = "bad"

                @multicache(key_prefix="blame", key_list=["ignore_blobs"])
                def blame(self, ignore_globs=None):
                    return None

    def test_valid_key_list_accepted(self):
        class Good:
            cache_backend = None
            repo_name = "good"

            @multicache(key_prefix="blame", key_list=["ignore_globs"])
            def blame(self, ignore_globs=None):
                return ignore_globs

        assert Good().blame(ignore_globs=["*.js"]) == ["*.js"]


class TestSkipBrokenInKey:
    """skip_broken changes both the rows returned and whether the method raises."""

    def test_skip_broken_variants_do_not_share_a_cache_entry(self, master_only_repo):
        cache = EphemeralCache()
        repo = Repository(working_dir=master_only_repo, cache_backend=cache)

        repo.revs(branch="master", skip_broken=True)
        repo.revs(branch="master", skip_broken=False)

        revs_keys = [k for k in cache._cache if k.startswith("revs||")]
        assert len(revs_keys) == 2

    def test_tags_skip_broken_variants_do_not_share_a_cache_entry(self, master_only_repo):
        cache = EphemeralCache()
        repo = Repository(working_dir=master_only_repo, cache_backend=cache)

        repo.tags(skip_broken=True)
        repo.tags(skip_broken=False)

        assert len([k for k in cache._cache if k.startswith("tags||")]) == 2
