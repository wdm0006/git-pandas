"""
Example demonstrating release analytics features.

This example shows how to use gitpandas to analyze changes between release tags.
"""

import pandas as pd

from gitpandas import Repository

# --- Instantiate Repository ---
# Using the gitpandas repository URI as an example
# Replace with your repository URI or local path
repo_uri = "https://github.com/wdm0006/git-pandas.git"
print(f"Attempting to instantiate Repository for: {repo_uri}")

try:
    # Instantiate the Repository object.
    # For remote repositories, gitpandas will clone it to a temporary directory.
    # verbose=True can be helpful for debugging, but is optional here.
    repo = Repository(working_dir=repo_uri, verbose=False)
    print(f"Successfully instantiated Repository for: {repo.repo_name}")
    print(f"Cloned to temporary directory: {repo.git_dir}")
except Exception as e:
    print(f"Error instantiating repository: {e}")
    repo = None  # Ensure repo is None if instantiation fails

# --- Call release_tag_summary ---
if repo:
    print("\n--- Release Tag Summary ---")
    try:
        # This method analyzes the repository's tags to provide a summary of
        # changes between each tagged release.
        # It looks at the differences from the previous tag to the current one.
        # Output includes diff statistics (insertions, deletions),
        # committers, authors, and files changed during that period.
        # You can use tag_glob to filter for specific tag patterns (e.g., 'v*.*').
        release_summary_df = repo.release_tag_summary()

        if not release_summary_df.empty:
            print("Release summary retrieved successfully:")
            # Display the DataFrame. Pandas default display might be wide,
            # but for an example, direct print is usually fine.
            # For better display in production, consider setting pandas display options
            print(release_summary_df)

            # Example of how to access specific information:
            if "tag" in release_summary_df.columns and len(release_summary_df) > 1:
                # Show files changed between the first two listed tags (if available)
                # Note: The first tag in the summary won't have "previous tag" data.
                second_tag_entry = release_summary_df.iloc[1]  # Second tag in the sorted list
                print(f"\nExample: Files changed for tag '{second_tag_entry['tag']}' (since previous tag):")
                if isinstance(second_tag_entry["files"], list) and second_tag_entry["files"]:
                    for file_path in second_tag_entry["files"]:
                        print(f"  - {file_path}")
                else:
                    print("  No files listed or files column is not a list.")

        elif release_summary_df is not None:  # Empty DataFrame
            print("No release summary data returned. The repository might not have tags, or no tags match the glob.")
        else:  # None was returned, indicating an issue
            print("Failed to retrieve release summary (method returned None).")

    except Exception as e:
        print(f"Error calling release_tag_summary: {e}")

    # --- Optional: Demonstrate get_commit_content ---
    # The release_tag_summary gives you information about *what* changed (files, stats).
    # If you need to see the *actual content* of a specific commit that was part of a
    # release (perhaps a commit listed by commits_in_tags, or the commit SHA
    # directly associated with a tag), you can use get_commit_content.

    print("\n--- Optional: Get Content of a Specific Commit ---")
    # Note: For a real scenario, you'd get a relevant commit SHA from your analysis,
    # for example, from the 'commit_sha' column in release_summary_df or from repo.commits_in_tags().
    # As this is a brief example, we'll try to pick one from the summary if possible,
    # otherwise, we'll use a placeholder.
    target_commit_sha = None
    if "release_summary_df" in locals() and not release_summary_df.empty and "commit_sha" in release_summary_df.columns:
        # Let's try to get the commit SHA of the first tag listed (if any)
        # This commit is what the tag points to.
        potential_sha = release_summary_df["commit_sha"].iloc[0]
        if pd.notna(potential_sha):  # Check if the SHA is not NaN or None
            target_commit_sha = potential_sha
            print(f"Attempting to get content for commit SHA (from first tag's commit_sha): {target_commit_sha}")
        else:
            print("Could not get a valid commit SHA from the release_summary_df's first entry.")

    if not target_commit_sha:
        target_commit_sha = "PLACEHOLDER_COMMIT_SHA"  # Replace with an actual commit SHA from the repo
        print(f"Using placeholder commit SHA: {target_commit_sha}. Replace with a real one for actual output.")

    if target_commit_sha != "PLACEHOLDER_COMMIT_SHA":
        try:
            # The 'rev' parameter takes the commit SHA.
            commit_content_df = repo.get_commit_content(rev=target_commit_sha)

            if commit_content_df is not None and not commit_content_df.empty:
                print(f"Content changes for commit {target_commit_sha} (showing first 5 lines):")
                # Displaying only a part of the DataFrame for brevity.
                # Columns typically include: 'file_path', 'change_type', 'diff', 'old_blob_sha', 'new_blob_sha'
                print(commit_content_df.head())
            elif commit_content_df is not None:  # Empty DataFrame
                print(
                    f"No content changes (e.g. diffs) found for commit {target_commit_sha}. "
                    f"This can be normal for merge commits with no textual changes, "
                    f"or if the commit only modified tree structure."
                )
            else:  # None was returned
                print(
                    f"Failed to get content for commit {target_commit_sha} (method returned None). "
                    f"Could be an invalid SHA or repository issue."
                )
        except Exception as e:
            print(f"Error calling get_commit_content for {target_commit_sha}: {e}")
    else:
        print("Skipping get_commit_content due to placeholder SHA or if no valid SHA was found.")

else:
    print("\nSkipping release_tag_summary and get_commit_content because repository instantiation failed.")
