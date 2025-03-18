"""
Example of analyzing blame information in parallel.

This example demonstrates:
1. Creating a repository instance
2. Analyzing blame information sequentially
3. Analyzing blame information in parallel
4. Comparing performance between sequential and parallel analysis
"""

from gitpandas import Repository
import time
from definitions import GIT_PANDAS_DIR

__author__ = 'willmcginnis'


if __name__ == '__main__':
    print("Initializing repository...")
    repo = Repository(working_dir=GIT_PANDAS_DIR)
    
    # Define analysis parameters
    branch = 'master'
    include_globs = ['*.py', '*.html', '*.sql', '*.md']
    limit = 50  # Limit to 50 commits for faster analysis
    skip = 2    # Skip every other commit
    
    print(f"\nAnalyzing blame information for {branch} branch")
    print(f"Including files: {', '.join(include_globs)}")
    print(f"Analyzing {limit} commits, skipping every {skip}th commit")
    
    # Sequential analysis
    print("\nRunning sequential analysis...")
    start_time = time.time()
    blame = repo.cumulative_blame(
        branch=branch,
        include_globs=include_globs,
        limit=limit,
        skip=skip
    )
    seq_time = time.time() - start_time
    print(f"Sequential analysis completed in {seq_time:.2f} seconds")
    print("\nSample of results:")
    print(blame.head())
    
    # Parallel analysis
    print("\nRunning parallel analysis with 4 workers...")
    start_time = time.time()
    blame = repo.parallel_cumulative_blame(
        branch=branch,
        include_globs=include_globs,
        limit=limit,
        skip=skip,
        workers=4
    )
    par_time = time.time() - start_time
    print(f"Parallel analysis completed in {par_time:.2f} seconds")
    print("\nSample of results:")
    print(blame.head())
    
    # Compare performance
    print(f"\nPerformance comparison:")
    print(f"  Sequential time: {seq_time:.2f} seconds")
    print(f"  Parallel time:   {par_time:.2f} seconds")
    print(f"  Speedup:         {seq_time/par_time:.2f}x")