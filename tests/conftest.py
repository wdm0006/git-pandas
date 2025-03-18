"""
Shared pytest fixtures for git-pandas tests.
"""

import os
import pytest
import git
from gitpandas import Repository, ProjectDirectory

__author__ = 'willmcginnis'

# Common test utilities can be added here if needed 

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    ) 