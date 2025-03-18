"""
Example of visualizing cumulative blame information.

This example demonstrates:
1. Creating a project directory instance
2. Analyzing blame information across multiple repositories
3. Visualizing the results using matplotlib
"""

import os
import time

from gitpandas import ProjectDirectory
from gitpandas.utilities.plotting import plot_cumulative_blame

__author__ = "willmcginnis"


if __name__ == "__main__":
    print("Initializing project directory...")
    start_time = time.time()

    # Use pygeohash repository - a good size for examples
    g = ProjectDirectory(working_dir=["https://github.com/wdm0006/pygeohash.git"])

    print("\nAnalyzing blame information...")
    print("Using a limit of 20 commits and skipping every 2nd commit for faster analysis")

    try:
        blame = g.cumulative_blame(
            branch="master",  # Use master instead of main
            include_globs=["*.py"],  # Focus on Python files only
            by="committer",
            limit=20,  # Limit to 20 commits
            skip=2,  # Skip every other commit
        )

        print("\nGenerating visualization...")
        # Create the plot and save it
        fig = plot_cumulative_blame(blame)
        output_path = os.path.join("img", "cumulative_blame.png")
        fig.savefig(output_path)
        print(f"Plot saved to {output_path}")

    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("This might happen if the repository is not accessible or if there are no commits to analyze.")

    end_time = time.time()
    print(f"\nAnalysis completed in {end_time - start_time:.2f} seconds")
