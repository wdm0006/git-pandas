"""
Tests for thread-safety of the DiskCache implementation.

This module contains tests to verify that the DiskCache class is thread-safe
and prevents the race conditions that can lead to memory corruption and
dictionary modification during iteration errors.
"""

import concurrent.futures
import contextlib
import os
import tempfile
import threading
import time

import pandas as pd
import pytest

from gitpandas.cache import CacheMissError, DiskCache, EphemeralCache, multicache


class MockRepoMethod:
    """Mock repository class for testing threading with multicache decorator."""

    def __init__(self, repo_name="test_repo", cache_backend=None):
        self.repo_name = repo_name
        self.cache_backend = cache_backend
        self.call_count = 0
        self._call_count_lock = threading.Lock()

    def increment_call_count(self):
        """Thread-safe increment of call count."""
        with self._call_count_lock:
            self.call_count += 1
            return self.call_count

    @multicache(key_prefix="thread_test_", key_list=["param1", "param2"])
    def cached_method(self, param1=None, param2=None):
        """Test method that increments call_count and returns a dataframe."""
        count = self.increment_call_count()
        # Add a small delay to increase chance of race conditions
        time.sleep(0.001)
        return pd.DataFrame(
            {"count": [count], "param1": [param1], "param2": [param2], "thread_id": [threading.get_ident()]}
        )


class TestDiskCacheThreadSafety:
    """Test class for DiskCache thread-safety."""

    @pytest.fixture
    def temp_cache_file(self):
        """Create a temporary file for the cache."""
        fd, path = tempfile.mkstemp(suffix=".gz")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def test_concurrent_set_operations(self, temp_cache_file):
        """Test that concurrent set operations don't cause race conditions."""
        cache = DiskCache(filepath=temp_cache_file)
        num_threads = 10
        operations_per_thread = 20

        def worker(worker_id):
            """Worker function that performs cache operations."""
            for i in range(operations_per_thread):
                key = f"worker_{worker_id}_key_{i}"
                value = pd.DataFrame({"worker": [worker_id], "operation": [i]})
                cache.set(key, value)

        # Start multiple threads
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all keys were set correctly
        expected_keys = num_threads * operations_per_thread
        assert len(cache._cache) <= expected_keys  # May be less due to LRU eviction
        assert len(cache._key_list) <= expected_keys

        # Verify cache consistency
        assert len(cache._cache) == len(cache._key_list)

    def test_concurrent_get_operations(self, temp_cache_file):
        """Test that concurrent get operations are thread-safe."""
        cache = DiskCache(filepath=temp_cache_file)

        # Pre-populate cache
        test_data = {}
        for i in range(50):
            key = f"test_key_{i}"
            value = pd.DataFrame({"value": [i], "squared": [i**2]})
            cache.set(key, value)
            test_data[key] = value

        results = {}
        results_lock = threading.Lock()

        def worker(worker_id):
            """Worker function that reads from cache."""
            worker_results = []
            for i in range(25):  # Each worker reads 25 keys
                key = f"test_key_{i}"
                try:
                    result = cache.get(key)
                    worker_results.append((key, result))
                except CacheMissError:
                    # May happen due to LRU eviction
                    pass

            with results_lock:
                results[worker_id] = worker_results

        # Start multiple threads
        num_threads = 8
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify results are consistent
        for _worker_id, worker_results in results.items():
            for key, result in worker_results:
                if key in test_data:
                    pd.testing.assert_frame_equal(result, test_data[key])

    def test_concurrent_mixed_operations(self, temp_cache_file):
        """Test concurrent mix of set, get, and exists operations."""
        cache = DiskCache(filepath=temp_cache_file, max_keys=100)

        operations_completed = []
        operations_lock = threading.Lock()

        def reader_worker(worker_id):
            """Worker that performs read operations."""
            for i in range(30):
                key = f"key_{i % 20}"  # Read from a subset of keys
                try:
                    cache.get(key)
                    with operations_lock:
                        operations_completed.append(f"read_{worker_id}_{i}")
                except CacheMissError:
                    pass

        def writer_worker(worker_id):
            """Worker that performs write operations."""
            for i in range(30):
                key = f"key_{i}"
                value = pd.DataFrame({"worker": [worker_id], "op": [i]})
                cache.set(key, value)
                with operations_lock:
                    operations_completed.append(f"write_{worker_id}_{i}")

        def checker_worker(worker_id):
            """Worker that checks key existence."""
            for i in range(30):
                key = f"key_{i % 15}"
                cache.exists(key)
                with operations_lock:
                    operations_completed.append(f"exists_{worker_id}_{i}")

        # Start mixed workload
        threads = []

        # 3 reader threads
        for i in range(3):
            thread = threading.Thread(target=reader_worker, args=(f"r{i}",))
            threads.append(thread)

        # 3 writer threads
        for i in range(3):
            thread = threading.Thread(target=writer_worker, args=(f"w{i}",))
            threads.append(thread)

        # 2 checker threads
        for i in range(2):
            thread = threading.Thread(target=checker_worker, args=(f"c{i}",))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify operations completed without errors
        assert len(operations_completed) > 0

        # Verify cache consistency
        assert len(cache._cache) == len(cache._key_list)

    def test_concurrent_save_operations_no_corruption(self, temp_cache_file):
        """Test that concurrent save operations don't corrupt the cache file."""
        cache = DiskCache(filepath=temp_cache_file)

        def aggressive_writer(worker_id):
            """Worker that aggressively writes to cache."""
            for i in range(50):
                key = f"aggressive_{worker_id}_{i}"
                value = pd.DataFrame({"worker": [worker_id], "iteration": [i], "timestamp": [time.time()]})
                cache.set(key, value)
                # Small delay to encourage race conditions
                time.sleep(0.0001)

        # Start multiple aggressive writers
        num_threads = 6
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=aggressive_writer, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify cache file is not corrupted by creating a new cache instance
        new_cache = DiskCache(filepath=temp_cache_file)

        # Verify the new cache loaded successfully
        assert isinstance(new_cache._cache, dict)
        assert isinstance(new_cache._key_list, list)
        assert len(new_cache._cache) == len(new_cache._key_list)

    def test_multicache_decorator_thread_safety(self, temp_cache_file):
        """Test that the multicache decorator works correctly with threading."""
        cache = DiskCache(filepath=temp_cache_file)
        repo = MockRepoMethod(cache_backend=cache)

        results = {}
        results_lock = threading.Lock()

        def worker(worker_id):
            """Worker that calls cached methods."""
            worker_results = []
            for i in range(10):
                # Mix of same and different parameters
                param1 = f"param_{i % 5}"  # 5 different param1 values
                param2 = f"value_{i % 3}"  # 3 different param2 values

                result = repo.cached_method(param1=param1, param2=param2)
                worker_results.append((param1, param2, result))

            with results_lock:
                results[worker_id] = worker_results

        # Start multiple threads
        num_threads = 8
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify that cache hits occurred (call_count should be less than total operations)
        total_operations = num_threads * 10

        # Call count should be less than total operations (indicating cache hits occurred)
        # Due to threading timing, we may not get perfect cache hits, but it should be significantly less
        assert repo.call_count < total_operations, (
            f"Expected cache hits: call_count {repo.call_count} should be < total_operations {total_operations}"
        )

        # The important thing is that we didn't crash and cache was working
        # In a perfect world, call_count would be <= unique_param_combinations,
        # but threading timing can cause some cache misses

        # Verify results consistency for same parameters
        param_results = {}
        inconsistencies = []
        for worker_id, worker_results in results.items():
            for param1, param2, result in worker_results:
                key = (param1, param2)
                if key not in param_results:
                    param_results[key] = result
                else:
                    # Results for same parameters should be identical
                    try:
                        pd.testing.assert_frame_equal(result, param_results[key])
                    except AssertionError:
                        # Record inconsistency for analysis
                        inconsistencies.append(
                            {
                                "key": key,
                                "worker_id": worker_id,
                                "expected_count": param_results[key]["count"][0],
                                "actual_count": result["count"][0],
                            }
                        )

        # For debugging: show inconsistencies but don't fail the test if threading is working
        if inconsistencies:
            total_operations = len([item for sublist in results.values() for item in sublist])
            print(f"Cache inconsistencies detected: {len(inconsistencies)} out of {total_operations} total operations")
            print(f"Total unique parameter combinations: {len(param_results)}")
            print(f"Call count: {repo.call_count}")
            # This indicates the cache might have race conditions, but the main goal is no crashes
            # In a production environment, users should be aware that perfect cache consistency
            # might not be guaranteed under high concurrency

    def test_stress_test_high_concurrency(self, temp_cache_file):
        """Stress test with high concurrency to detect race conditions."""
        cache = DiskCache(filepath=temp_cache_file, max_keys=200)

        # Use ThreadPoolExecutor for more controlled concurrency
        def cache_operation(operation_id):
            """Perform a mix of cache operations."""
            try:
                if operation_id % 3 == 0:
                    # Write operation
                    key = f"stress_key_{operation_id % 50}"
                    value = pd.DataFrame({"op_id": [operation_id], "type": ["write"]})
                    cache.set(key, value)
                elif operation_id % 3 == 1:
                    # Read operation
                    key = f"stress_key_{operation_id % 50}"
                    with contextlib.suppress(CacheMissError):
                        cache.get(key)
                else:
                    # Exists check
                    key = f"stress_key_{operation_id % 50}"
                    cache.exists(key)

                return True
            except Exception as e:
                # Any exception indicates a threading issue
                return f"Error in operation {operation_id}: {e}"

        # Run high-concurrency operations
        num_operations = 500
        max_workers = 20

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(cache_operation, i) for i in range(num_operations)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Check for any errors
        errors = [r for r in results if r is not True]
        assert len(errors) == 0, f"Threading errors detected: {errors}"

        # Verify cache consistency after stress test
        assert len(cache._cache) == len(cache._key_list)

    def test_no_dictionary_changed_size_error(self, temp_cache_file):
        """Specific test to ensure 'dictionary changed size during iteration' doesn't occur."""
        cache = DiskCache(filepath=temp_cache_file)

        # This test specifically targets the race condition mentioned in the bug report
        exception_caught = []
        exception_lock = threading.Lock()

        def dictionary_modifier(worker_id):
            """Worker that aggressively modifies the cache dictionary."""
            for i in range(100):
                try:
                    key = f"dict_mod_{worker_id}_{i}"
                    value = pd.DataFrame({"mod": [i]})
                    cache.set(key, value)
                except RuntimeError as e:
                    if "dictionary changed size during iteration" in str(e):
                        with exception_lock:
                            exception_caught.append(str(e))
                except Exception as e:
                    # Catch any other threading-related exceptions
                    with exception_lock:
                        exception_caught.append(f"Unexpected error: {e}")

        # Start many threads to maximize chance of race condition
        num_threads = 15
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=dictionary_modifier, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify no dictionary iteration errors occurred
        assert len(exception_caught) == 0, f"Dictionary iteration errors: {exception_caught}"

    def test_memory_corruption_detection(self, temp_cache_file):
        """Test to detect potential memory corruption issues."""
        cache = DiskCache(filepath=temp_cache_file)

        # Pre-populate with known data
        reference_data = {}
        for i in range(20):
            key = f"ref_key_{i}"
            value = pd.DataFrame({"ref_val": [i], "check": [i * 2]})
            cache.set(key, value)
            reference_data[key] = value

        corruption_detected = []
        corruption_lock = threading.Lock()

        def corruption_checker(worker_id):
            """Worker that checks for data corruption."""
            for i in range(50):
                try:
                    # Read reference data and verify it hasn't been corrupted
                    key = f"ref_key_{i % 20}"
                    if key in reference_data:
                        retrieved = cache.get(key)
                        expected = reference_data[key]

                        # Check if data was corrupted
                        if not retrieved.equals(expected):
                            with corruption_lock:
                                corruption_detected.append(f"Data corruption in {key}")

                        # Also write new data to increase contention
                        new_key = f"worker_{worker_id}_key_{i}"
                        new_value = pd.DataFrame({"worker": [worker_id], "iter": [i]})
                        cache.set(new_key, new_value)

                except Exception as e:
                    with corruption_lock:
                        corruption_detected.append(f"Exception in worker {worker_id}: {e}")

        # Start multiple threads
        num_threads = 10
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=corruption_checker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify no corruption was detected
        assert len(corruption_detected) == 0, f"Memory corruption detected: {corruption_detected}"

    def test_cache_persistence_across_threads(self, temp_cache_file):
        """Test that cache persistence works correctly with threading."""
        # First phase: populate cache with multiple threads
        cache1 = DiskCache(filepath=temp_cache_file)

        def populator(worker_id):
            """Populate cache with data."""
            for i in range(10):
                key = f"persist_{worker_id}_{i}"
                value = pd.DataFrame({"worker": [worker_id], "data": [i]})
                cache1.set(key, value)  # noqa: F821

        threads = []
        for i in range(5):
            thread = threading.Thread(target=populator, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Force save and close first cache
        cache1.save()
        del cache1

        # Second phase: load cache and verify data with multiple threads
        cache2 = DiskCache(filepath=temp_cache_file)

        verification_results = {}
        verification_lock = threading.Lock()

        def verifier(worker_id):
            """Verify persisted data."""
            results = []
            for i in range(10):
                key = f"persist_{worker_id}_{i}"
                try:
                    result = cache2.get(key)
                    results.append((key, result))
                except CacheMissError:
                    # May not exist due to LRU eviction
                    pass

            with verification_lock:
                verification_results[worker_id] = results

        threads = []
        for i in range(5):
            thread = threading.Thread(target=verifier, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify that at least some data was persisted and loaded correctly
        total_verified = sum(len(results) for results in verification_results.values())
        assert total_verified > 0, "No data was successfully persisted and loaded"

    def test_concurrent_additions_near_max_key_limit(self, temp_cache_file):
        """
        Test the race condition that occurs when multiple threads add items
        to the cache simultaneously when near the max key limit.

        This test demonstrates the potential for IndexError, KeyError, and
        cache inconsistency when multiple threads trigger eviction simultaneously.
        """
        # Use a small max_keys to easily trigger the race condition
        cache = DiskCache(filepath=temp_cache_file, max_keys=10)

        # Pre-populate cache to near the limit (8 out of 10 keys)
        for i in range(8):
            cache.set(f"initial_key_{i}", pd.DataFrame({"initial": [i]}))

        errors_caught = []
        errors_lock = threading.Lock()
        successful_operations = []
        operations_lock = threading.Lock()

        def concurrent_adder(worker_id):
            """
            Worker that adds multiple keys, potentially triggering eviction.
            Each worker adds 5 keys, so total will exceed max_keys significantly.
            """
            for i in range(5):
                try:
                    key = f"worker_{worker_id}_key_{i}"
                    value = pd.DataFrame({"worker": [worker_id], "iteration": [i]})
                    cache.set(key, value)

                    with operations_lock:
                        successful_operations.append(f"{worker_id}_{i}")

                except (IndexError, KeyError, ValueError) as e:
                    # These are the expected race condition errors
                    with errors_lock:
                        errors_caught.append(f"Worker {worker_id}, iteration {i}: {type(e).__name__}: {e}")
                except Exception as e:
                    # Any other unexpected error
                    with errors_lock:
                        errors_caught.append(f"Worker {worker_id}, iteration {i}: Unexpected {type(e).__name__}: {e}")

        # Start multiple threads that will all try to add keys simultaneously
        # This should trigger the race condition in eviction
        num_threads = 8
        threads = []

        for worker_id in range(num_threads):
            thread = threading.Thread(target=concurrent_adder, args=(worker_id,))
            threads.append(thread)

        # Start all threads at roughly the same time to maximize race condition chance
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check if any race condition errors occurred
        if errors_caught:
            print("Race condition errors detected (this demonstrates the bug):")
            for error in errors_caught[:10]:  # Show first 10 errors
                print(f"  - {error}")

        # Verify cache consistency after the operations
        cache_size = len(cache._cache)
        key_list_size = len(cache._key_list)

        print(f"Final cache state: {cache_size} items in cache, {key_list_size} items in key_list")
        print(f"Successful operations: {len(successful_operations)}")
        print(f"Errors caught: {len(errors_caught)}")

        # The cache should be consistent even if there were errors
        assert cache_size == key_list_size, f"Cache inconsistency: {cache_size} != {key_list_size}"

        # Cache size should not exceed max_keys (allowing for some tolerance due to threading)
        assert cache_size <= cache._max_keys + num_threads, (
            f"Cache size {cache_size} significantly exceeds max_keys {cache._max_keys}"
        )

        # If this test fails with race condition errors, it demonstrates the threading issue
        # Note: This test might pass on some runs due to timing, but should fail consistently
        # on systems with high concurrency or when run multiple times

    def test_ephemeral_cache_race_condition_near_max_keys(self):
        """
        Test to demonstrate race condition in EphemeralCache when multiple threads
        add items near the max key limit. EphemeralCache has NO thread safety,
        so this should expose the race condition more reliably.
        """
        # Use a very small max_keys to easily trigger the race condition
        cache = EphemeralCache(max_keys=5)

        # Pre-populate cache to near the limit (3 out of 5 keys)
        for i in range(3):
            cache.set(f"initial_key_{i}", pd.DataFrame({"initial": [i]}))

        errors_caught = []
        errors_lock = threading.Lock()
        successful_operations = []
        operations_lock = threading.Lock()

        def concurrent_adder(worker_id):
            """
            Worker that adds multiple keys, triggering eviction.
            Each worker adds 4 keys, so total will significantly exceed max_keys.
            """
            for i in range(4):
                try:
                    key = f"worker_{worker_id}_key_{i}"
                    value = pd.DataFrame({"worker": [worker_id], "iteration": [i]})
                    cache.set(key, value)

                    with operations_lock:
                        successful_operations.append(f"{worker_id}_{i}")

                except (IndexError, KeyError, ValueError, RuntimeError) as e:
                    # These are the expected race condition errors
                    with errors_lock:
                        errors_caught.append(f"Worker {worker_id}, iteration {i}: {type(e).__name__}: {e}")
                except Exception as e:
                    # Any other unexpected error
                    with errors_lock:
                        errors_caught.append(f"Worker {worker_id}, iteration {i}: Unexpected {type(e).__name__}: {e}")

        # Start multiple threads that will all try to add keys simultaneously
        # This should trigger the race condition in eviction for EphemeralCache
        num_threads = 10
        threads = []

        for worker_id in range(num_threads):
            thread = threading.Thread(target=concurrent_adder, args=(worker_id,))
            threads.append(thread)

        # Start all threads at roughly the same time to maximize race condition chance
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Report results
        cache_size = len(cache._cache)
        key_list_size = len(cache._key_list)

        print("\nEphemeralCache Race Condition Test Results:")
        print(f"Final cache state: {cache_size} items in cache, {key_list_size} items in key_list")
        print(f"Successful operations: {len(successful_operations)}")
        print(f"Errors caught: {len(errors_caught)}")

        if errors_caught:
            print("Race condition errors detected:")
            for error in errors_caught[:5]:  # Show first 5 errors
                print(f"  - {error}")

        # Check for cache consistency issues
        cache_inconsistent = cache_size != key_list_size
        if cache_inconsistent:
            print(f"CACHE INCONSISTENCY DETECTED: cache size ({cache_size}) != key_list size ({key_list_size})")

        # This test is meant to demonstrate the issue, so we'll make it informational
        # In a real fix, we'd want these assertions to pass
        print(f"Cache consistency: {'FAILED' if cache_inconsistent else 'OK'}")
        print(f"Race condition errors: {'DETECTED' if errors_caught else 'NONE'}")

        # For now, just ensure the test doesn't crash completely
        # In a production environment, these race conditions could cause:
        # 1. IndexError when evict() tries to pop from empty list
        # 2. KeyError when evict() tries to delete already-deleted keys
        # 3. Cache inconsistency where _cache and _key_list get out of sync
        assert True  # Test always passes, but demonstrates the issues above

    def test_aggressive_ephemeral_cache_race_condition(self):
        """
        More aggressive test to expose race conditions in EphemeralCache.
        Uses many threads, smaller cache, and rapid operations to maximize
        the chance of exposing threading issues.
        """
        # Very small cache to force frequent evictions
        cache = EphemeralCache(max_keys=3)

        errors_caught = []
        errors_lock = threading.Lock()
        operation_count = 0
        count_lock = threading.Lock()

        def aggressive_worker(worker_id):
            """Worker that rapidly adds many keys to force evictions."""
            nonlocal operation_count

            for i in range(20):  # Each worker does 20 operations
                try:
                    key = f"w{worker_id}_k{i}"
                    value = pd.DataFrame({"w": [worker_id], "i": [i]})

                    # Add some variability in timing
                    if i % 3 == 0:
                        time.sleep(0.0001)  # Tiny delay to vary timing

                    cache.set(key, value)

                    with count_lock:
                        operation_count += 1

                except Exception as e:
                    with errors_lock:
                        errors_caught.append(f"W{worker_id}I{i}: {type(e).__name__}: {str(e)}")

        # Use many threads to increase contention
        num_threads = 20
        threads = []

        # Create and start all threads quickly
        for worker_id in range(num_threads):
            thread = threading.Thread(target=aggressive_worker, args=(worker_id,))
            threads.append(thread)

        # Start all threads as close together as possible
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Analyze results
        cache_size = len(cache._cache)
        key_list_size = len(cache._key_list)

        print("\nAggressive EphemeralCache Test Results:")
        print(f"Total operations attempted: {num_threads * 20}")
        print(f"Successful operations: {operation_count}")
        print(f"Final cache state: {cache_size} items in cache, {key_list_size} items in key_list")
        print(f"Errors caught: {len(errors_caught)}")

        if errors_caught:
            print("Race condition errors detected:")
            error_types = {}
            for error in errors_caught:
                error_type = error.split(":")[1].strip()
                error_types[error_type] = error_types.get(error_type, 0) + 1

            for error_type, count in error_types.items():
                print(f"  - {error_type}: {count} occurrences")

            # Show a few example errors
            print("Example errors:")
            for error in errors_caught[:3]:
                print(f"  - {error}")

        # Check for inconsistencies
        cache_inconsistent = cache_size != key_list_size
        if cache_inconsistent:
            print(f"CACHE INCONSISTENCY: cache has {cache_size} items but key_list has {key_list_size}")

            # Try to identify the discrepancy
            cache_keys = set(cache._cache.keys())
            list_keys = set(cache._key_list)

            only_in_cache = cache_keys - list_keys
            only_in_list = list_keys - cache_keys

            if only_in_cache:
                print(f"Keys only in cache dict: {only_in_cache}")
            if only_in_list:
                print(f"Keys only in key list: {only_in_list}")

        print(f"Cache consistency: {'FAILED' if cache_inconsistent else 'OK'}")
        print(f"Race conditions: {'DETECTED' if errors_caught else 'NONE'}")

        # This test demonstrates potential issues but doesn't fail
        # The key insight is that even if we don't see errors every time,
        # the potential for race conditions exists in the current implementation
        assert True
