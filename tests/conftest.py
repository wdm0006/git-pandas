"""
Shared pytest fixtures for git-pandas tests.
"""

import subprocess
import pytest

__author__ = "willmcginnis"


def get_default_branch():
    """Get the system's default branch name for new repositories."""
    try:
        # Try to get the configured default branch
        result = subprocess.run(
            ["git", "config", "--global", "init.defaultBranch"],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    
    # If no default branch is configured, create a temporary repo to see what git actually creates
    import tempfile
    import os
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            test_repo_path = os.path.join(temp_dir, "test_repo")
            
            # Initialize a repo and make an initial commit to see what branch git creates
            init_result = subprocess.run(
                ["git", "init", test_repo_path],
                capture_output=True,
                text=True,
                check=False
            )
            
            if init_result.returncode == 0:
                # Configure user for the test repo
                subprocess.run(
                    ["git", "-C", test_repo_path, "config", "user.name", "Test"],
                    capture_output=True,
                    check=False
                )
                subprocess.run(
                    ["git", "-C", test_repo_path, "config", "user.email", "test@example.com"],
                    capture_output=True,
                    check=False
                )
                
                # Create a file and commit to establish a branch
                test_file = os.path.join(test_repo_path, "test.txt")
                with open(test_file, "w") as f:
                    f.write("test")
                
                subprocess.run(
                    ["git", "-C", test_repo_path, "add", "test.txt"],
                    capture_output=True,
                    check=False
                )
                subprocess.run(
                    ["git", "-C", test_repo_path, "commit", "-m", "initial"],
                    capture_output=True,
                    check=False
                )
                
                # Check what branch was created
                branch_result = subprocess.run(
                    ["git", "-C", test_repo_path, "branch", "--show-current"],
                    capture_output=True,
                    text=True,
                    check=False
                )
                
                if branch_result.returncode == 0 and branch_result.stdout.strip():
                    return branch_result.stdout.strip()
    except Exception:
        pass
    
    # Final fallback to 'master' for maximum compatibility
    return "master"


@pytest.fixture(scope="session")
def default_branch():
    """Pytest fixture to get the default branch name."""
    return get_default_branch()


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
