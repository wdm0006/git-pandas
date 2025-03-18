"""
Example of analyzing commit history in a repository.

This example demonstrates:
1. Creating repository and project directory instances
2. Analyzing commit history with reasonable limits
3. Viewing committer statistics
4. Analyzing file changes by extension
"""

import os
import time

import numpy as np
from definitions import GIT_PANDAS_DIR
from pandas import set_option

from gitpandas import ProjectDirectory, Repository

__author__ = "willmcginnis"


def project(path):
    """Analyze commit history for a project directory."""
    print("\nAnalyzing project directory...")
    start_time = time.time()

    p = ProjectDirectory(working_dir=path)

    # Get commit history with reasonable limits
    print("\nGetting commit history (last 7 days, limited to 100 commits)...")
    ch = p.commit_history(
        "master",
        limit=100,
        include_globs=["*.py"],
        ignore_globs=["lib/*", "docs/*", "test/*", "tests/*", "tests_t/*"],
        days=7,
    )
    print("\nRecent commits:")
    print(ch.head())

    # Get committer statistics
    committers = set(ch["committer"].values)
    print(f"\nFound {len(committers)} unique committers:")
    for committer in sorted(committers):
        print(f"  - {committer}")

    # Calculate contributions
    print("\nContributions by committer:")
    attr = ch.reindex(columns=["committer", "lines", "insertions", "deletions", "net"]).groupby(["committer"])
    attr = attr.agg({"lines": np.sum, "insertions": np.sum, "deletions": np.sum, "net": np.sum})
    print(attr)

    print(f"\nProject analysis completed in {time.time() - start_time:.2f} seconds")


def repository(path):
    """Analyze commit history for a single repository."""
    print("\nAnalyzing repository...")
    start_time = time.time()

    # Build repository object
    ignore_dirs = ["docs/*", "tests/*", "Data/*"]
    r = Repository(path)

    # Check if bare
    print("\nRepository type:")
    print(f"  Bare repository: {r.is_bare()}")

    # Get commit history with limits
    print("\nGetting commit history (limited to 50 commits)...")
    ch = r.commit_history("HEAD", limit=50, include_globs=["*.py"], ignore_globs=ignore_dirs)
    print("\nRecent commits:")
    print(ch.head(5))

    # Get committer statistics
    committers = set(ch["committer"].values)
    print(f"\nFound {len(committers)} unique committers:")
    for committer in sorted(committers):
        print(f"  - {committer}")

    # Calculate contributions
    print("\nContributions by committer:")
    attr = ch.reindex(columns=["committer", "lines", "insertions", "deletions"]).groupby(["committer"])
    attr = attr.agg({"lines": np.sum, "insertions": np.sum, "deletions": np.sum})
    print(attr)

    # Get file change history with limits
    print("\nAnalyzing file changes (limited to 50 commits)...")
    fh = r.file_change_history("HEAD", limit=50, ignore_globs=ignore_dirs)
    fh["ext"] = fh["filename"].map(lambda x: x.split(".")[-1])
    print("\nRecent file changes:")
    print(fh.head(10))

    # Analyze by extension
    print("\nChanges by file extension:")
    etns = fh.reindex(columns=["ext", "insertions", "deletions"]).groupby(["ext"])
    etns = etns.agg({"insertions": np.sum, "deletions": np.sum})
    print(etns)

    print(f"\nRepository analysis completed in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    # Configure pandas display options
    set_option("display.max_rows", 500)
    set_option("display.max_columns", 500)
    set_option("display.width", 1000)

    path = os.path.abspath(GIT_PANDAS_DIR)
    project(path)
    repository(path)
