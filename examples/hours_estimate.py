"""
Example of estimating development hours from commit history.

This example demonstrates:
1. Creating a repository instance
2. Analyzing commit history
3. Estimating development hours based on commit patterns
4. Visualizing the results
"""

import time

from gitpandas import Repository

__author__ = "willmcginnis"


if __name__ == "__main__":
    print("Initializing repository...")
    start_time = time.time()

    # Use pygeohash repository - a good size for examples
    repo = Repository(working_dir="https://github.com/wdm0006/pygeohash.git")

    print("\nAnalyzing commit history...")
    print("Using a limit of 20 commits for faster analysis")

    # Get commit history with limits
    commits = repo.commit_history(
        branch="master",  # Use master instead of main
        limit=20,  # Limit to 20 commits
        include_globs=["*.py"],  # Focus on Python files only
    )

    print("\nEstimating development hours...")
    # Group commits by day and estimate hours
    daily_hours = commits.groupby(commits.index.date).agg({"lines": "sum", "insertions": "sum", "deletions": "sum"})

    # Estimate hours based on commit patterns
    # Assuming average of 10 lines per hour of development
    daily_hours["estimated_hours"] = daily_hours["lines"] / 10

    print("\nResults:")
    print("\nDaily Development Hours:")
    print(daily_hours["estimated_hours"].round(2))

    print("\nSummary Statistics:")
    print(f"Total commits analyzed: {len(commits)}")
    print(f"Total days with commits: {len(daily_hours)}")
    print(f"Total estimated hours: {daily_hours['estimated_hours'].sum():.2f}")
    print(f"Average hours per day: {daily_hours['estimated_hours'].mean():.2f}")

    end_time = time.time()
    print(f"\nAnalysis completed in {end_time - start_time:.2f} seconds")
