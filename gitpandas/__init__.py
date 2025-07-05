from gitpandas.project import GitHubProfile, ProjectDirectory
from gitpandas.repository import Repository

try:
    from importlib.metadata import version

    __version__ = version("git-pandas")
except ImportError:
    # Fallback for Python < 3.8
    from importlib_metadata import version

    __version__ = version("git-pandas")

__author__ = "willmcginnis"

__all__ = ["Repository", "ProjectDirectory", "GitHubProfile"]
