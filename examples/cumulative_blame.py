"""
Example of visualizing cumulative blame information.

This example demonstrates:
1. Creating a project directory instance
2. Analyzing blame information across multiple repositories
3. Visualizing the results using matplotlib
"""

from gitpandas.utilities.plotting import plot_cumulative_blame
from gitpandas import ProjectDirectory
import time
import os

__author__ = 'willmcginnis'


if __name__ == '__main__':
    print("Initializing project directory...")
    start_time = time.time()
    
    # Use a repository that we know works well for examples
    g = ProjectDirectory(working_dir=['https://github.com/pandas-dev/pandas.git'])
    
    print("\nAnalyzing blame information...")
    print("Using a limit of 50 commits and skipping every 5th commit for faster analysis")
    
    try:
        blame = g.cumulative_blame(
            branch='main',
            include_globs=['*.py'],  # Focus on Python files only
            by='committer',
            limit=50,  # Limit to 50 commits
            skip=5     # Skip every 5th commit
        )
        
        print("\nGenerating visualization...")
        # Create the plot and save it
        fig = plot_cumulative_blame(blame)
        output_path = os.path.join('img', 'cumulative_blame.png')
        fig.savefig(output_path)
        print(f"Plot saved to {output_path}")
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("This might happen if the repository is not accessible or if there are no commits to analyze.")
    
    end_time = time.time()
    print(f"\nAnalysis completed in {end_time - start_time:.2f} seconds")