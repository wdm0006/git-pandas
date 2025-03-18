"""
Example of analyzing file lifelines and ownership changes.

This example demonstrates:
1. Creating a repository instance
2. Analyzing file change history
3. Identifying ownership changes and refactoring events
4. Visualizing survival curves for file owners
"""

from gitpandas import Repository
from gitpandas.utilities.plotting import plot_lifeline
import time
import os

__author__ = 'willmcginnis'


if __name__ == '__main__':
    print("Initializing repository...")
    start_time = time.time()
    
    # Use a smaller repository for faster analysis
    repo = Repository(working_dir='https://github.com/wdm0006/cookiecutter-pipproject.git')
    
    print("\nAnalyzing file change history...")
    print("Using a limit of 50 commits for faster analysis")
    
    # Get file change history with limits
    changes = repo.file_change_history(
        branch='main',
        limit=50,  # Limit to 50 commits
        skip=5     # Skip every 5th commit
    )
    
    print("\nIdentifying ownership changes...")
    # Identify ownership changes
    ownership_changes = changes.ownership_changes()
    
    print("\nIdentifying refactoring events...")
    # Identify refactoring events
    refactoring = changes.refactoring_events()
    
    print("\nGenerating visualization...")
    # Create the plot and save it
    fig = plot_lifeline(changes, ownership_changes, refactoring)
    output_path = os.path.join('img', 'lifeline.png')
    fig.savefig(output_path)
    print(f"Plot saved to {output_path}")
    
    end_time = time.time()
    print(f"\nAnalysis completed in {end_time - start_time:.2f} seconds")
    
    # Print summary statistics
    print(f"\nSummary:")
    print(f"Total files analyzed: {len(changes.file.unique())}")
    print(f"Total ownership changes: {len(ownership_changes)}")
    print(f"Total refactoring events: {len(refactoring)}")



