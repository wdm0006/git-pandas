"""
Example of analyzing blame information in parallel.

This example demonstrates:
1. Creating a repository instance
2. Analyzing blame information sequentially
3. Analyzing blame information in parallel (if joblib is available)
4. Comparing performance between sequential and parallel analysis
"""

import sys
import time

from gitpandas import Repository

__author__ = "willmcginnis"


if __name__ == "__main__":
    print("Initializing repository...")

    # Use pygeohash repository - a good size for examples
    repo = Repository(working_dir="https://github.com/wdm0006/pygeohash.git")

    # Define analysis parameters
    branch = "master"  # Use master instead of main
    include_globs = ["*.py"]  # Focus on Python files only
    limit = 20  # Limit to 20 commits for faster analysis

    print(f"\nAnalyzing blame information for {branch} branch")
    print(f"Including files: {', '.join(include_globs)}")
    print(f"Analyzing {limit} commits")

    # Sequential analysis
    print("\nRunning sequential analysis...")
    start_time = time.time()
    blame = repo.cumulative_blame(branch=branch, include_globs=include_globs, limit=limit)
    seq_time = time.time() - start_time
    print(f"Sequential analysis completed in {seq_time:.2f} seconds")
    print("\nSample of results:")
    print(blame.head())

    # Try parallel analysis if joblib is available
    try:
        import joblib  # noqa: F401

        print("\nRunning parallel analysis with 4 workers...")
        start_time = time.time()
        blame = repo.parallel_cumulative_blame(branch=branch, include_globs=include_globs, limit=limit, workers=4)
        par_time = time.time() - start_time
        print(f"Parallel analysis completed in {par_time:.2f} seconds")
        print("\nSample of results:")
        print(blame.head())

        # Compare performance
        print("\nPerformance comparison:")
        print(f"  Sequential time: {seq_time:.2f} seconds")
        print(f"  Parallel time:   {par_time:.2f} seconds")
        print(f"  Speedup:         {seq_time / par_time:.2f}x")
    except ImportError:
        print("\nParallel analysis skipped: joblib package not installed.")
        print("To enable parallel analysis, install joblib:")
        print("  pip install joblib")
        sys.exit(0)  # Exit with success since this is an expected case
