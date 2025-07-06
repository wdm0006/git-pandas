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
    
    # If no default branch is configured, fall back to system default
    # Most modern git versions use 'main', older versions use 'master'
    try:
        # Create a temporary repo to see what branch name git actually uses
        result = subprocess.run(
            ["git", "init", "--bare", "/tmp/test_default_branch_detection"],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0:
            # Clean up immediately
            subprocess.run(["rm", "-rf", "/tmp/test_default_branch_detection"], check=False)
            # Check git version to determine likely default
            version_result = subprocess.run(
                ["git", "--version"],
                capture_output=True,
                text=True,
                check=False
            )
            if version_result.returncode == 0:
                version_output = version_result.stdout
                # Git 2.28+ defaults to 'main' if no config is set
                if "git version 2.28" in version_output or "git version 2.2" in version_output:
                    return "main"
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
