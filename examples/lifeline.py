"""
Example of analyzing file lifelines and ownership changes.

This example demonstrates:
1. Creating a repository instance
2. Analyzing file change history
3. Identifying ownership changes and refactoring events
4. Visualizing survival curves for file owners
"""

import os
import time

import matplotlib

matplotlib.use("Agg")  # Set the backend to Agg before importing pyplot

from gitpandas import Repository
from gitpandas.utilities.plotting import plot_lifeline

__author__ = "willmcginnis"


if __name__ == "__main__":
    print("Initializing repository...")
    start_time = time.time()

    # Use pygeohash repository - a good size for examples
    repo = Repository(working_dir="https://github.com/wdm0006/pygeohash.git")

    print("\nAnalyzing file change history...")
    print("Using a limit of 20 commits for faster analysis")

    # Get file change history with limits
    changes = repo.file_change_history(
        branch="master",  # Use master instead of main
        limit=20,  # Limit to 20 commits
        include_globs=["*.py"],  # Focus on Python files only
    )

    print("\nDataFrame structure:")
    print("\nColumns:", changes.columns.tolist())
    print("\nSample data:")
    print(changes.head())

    print("\nIdentifying ownership changes...")
    # Identify ownership changes
    ownership_changes = changes.groupby("filename").filter(lambda x: len(x["committer"].unique()) > 1)

    print("\nIdentifying refactoring events...")
    # Identify refactoring events (significant changes to files)
    # Consider changes with >50 total lines changed (insertions + deletions) as refactoring
    changes["total_changes"] = changes["insertions"] + changes["deletions"]
    refactoring = changes[changes["total_changes"] > 50]

    print("\nGenerating visualization...")
    # Create the plot and save it
    fig = plot_lifeline(changes, ownership_changes, refactoring)
    output_path = os.path.join("img", "lifeline.png")
    fig.savefig(output_path)
    print(f"Plot saved to {output_path}")

    end_time = time.time()
    print(f"\nAnalysis completed in {end_time - start_time:.2f} seconds")

    # Print summary statistics
    print("\nSummary:")
    print(f"Total files analyzed: {len(changes.filename.unique())}")
    print(f"Total ownership changes: {len(ownership_changes)}")
    print(f"Total refactoring events: {len(refactoring)}")
    print("\nRefactoring events details:")
    print(refactoring[["filename", "total_changes", "message"]].to_string())
