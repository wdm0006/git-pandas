"""Configuration management for GitNOC Desktop."""

from .loader import load_repositories, save_repositories

__all__ = [
    "load_repositories",
    "save_repositories",
]
