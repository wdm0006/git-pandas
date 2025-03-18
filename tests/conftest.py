"""
Shared pytest fixtures for git-pandas tests.
"""

__author__ = "willmcginnis"

# Common test utilities can be added here if needed


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
