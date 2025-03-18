"""
Example of analyzing the "bus factor" of a repository.

The bus factor is a measure of risk based on how concentrated the codebase knowledge is
among contributors. A low bus factor (e.g. 1-2) indicates high risk as knowledge is
concentrated among few contributors.

This example demonstrates:
1. Creating a repository instance
2. Analyzing commit history with limits
3. Calculating bus factor
4. Viewing contributor statistics
"""

import time

from gitpandas import Repository

__author__ = "willmcginnis"


if __name__ == "__main__":
    # Use a smaller repository for faster analysis
    # This is a small Python package that's good for examples
    repo = Repository(working_dir="https://github.com/wdm0006/cookiecutter-pipproject.git")

    print("Analyzing repository...")
    start_time = time.time()

    # Get commit history with a reasonable limit
    print("\nGetting commit history (limited to last 100 commits)...")
    ch = repo.commit_history("master", limit=100, include_globs=["*.py"])

    # Calculate unique committers
    committers = set(ch["committer"].values)
    print(f"\nFound {len(committers)} unique committers:")
    for committer in sorted(committers):
        print(f"  - {committer}")

    # Calculate bus factor
    print("\nCalculating bus factor...")
    bus_factor = repo.bus_factor(include_globs=["*.py"])
    print("\nBus factor analysis:")
    print(bus_factor)

    end_time = time.time()
    print(f"\nAnalysis completed in {end_time - start_time:.2f} seconds")
