"""
Tests to verify that all example scripts run without errors.
"""

import subprocess
import sys
from pathlib import Path

import pytest

# Get the examples directory
EXAMPLES_DIR = Path(__file__).parent.parent / "examples"

# List of example scripts to test
EXAMPLE_SCRIPTS = [
    "attributes.py",
    "bus_analysis.py",
    "cloud_repo.py",
    "commit_history.py",
    "cumulative_blame.py",
    "definitions.py",
    "file_change_rates.py",
    "hours_estimate.py",
    "lifeline.py",
    "parallel_blame.py",
    "project_blame.py",
    "punchcard.py",
    "remote_fetch_and_cache_warming.py",  # Added new example
    "repo_file_detail.py",
    "release_analytics.py",  # Added new example
]


@pytest.mark.slow
def test_example_scripts():
    """Test that all example scripts run without errors."""
    for script in EXAMPLE_SCRIPTS:
        script_path = EXAMPLES_DIR / script
        assert script_path.exists(), f"Example script {script} not found"

        # Run the script with Python
        try:
            # Use the same Python interpreter that's running the tests
            python_executable = sys.executable
            result = subprocess.run(
                [python_executable, str(script_path)],
                cwd=EXAMPLES_DIR,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout per script
            )

            # Check if the script ran successfully
            assert result.returncode == 0, (
                f"Script {script} failed with return code {result.returncode}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

        except subprocess.TimeoutExpired:
            pytest.fail(f"Script {script} timed out after 5 minutes")
        except Exception as e:
            pytest.fail(f"Error running script {script}: {str(e)}")
