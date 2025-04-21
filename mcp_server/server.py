# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mcp[cli]",
#   "git-pandas",
#   "uvicorn",
# ]
# ///

import inspect
import logging
import os
import pathlib
from collections.abc import Callable
from functools import wraps
from typing import Any

import numpy as np
import pandas as pd
from mcp.server.fastmcp import FastMCP

import gitpandas as gp

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --- Configuration ---
class Config:
    """Server configuration settings."""

    # Directory to scan for Git repositories
    SCAN_ROOT_DIR: str = os.path.expanduser("~/Documents")
    # Directories to skip during repository scanning
    SKIP_DIRS: list[str] = [".git", ".svn", "node_modules", "venv", ".venv", "__pycache__"]
    # Maximum number of repositories to cache
    MAX_REPOS: int = 1000


# Global cache to store discovered repositories: {repo_name: gitpandas.Repository object}
REPO_CACHE: dict[str, gp.Repository] = {}

# --- Repository Management ---


class RepositoryError(Exception):
    """Base exception for repository-related errors."""

    pass


class RepositoryNotFoundError(RepositoryError):
    """Raised when a requested repository is not found in the cache."""

    pass


class RepositoryInitError(RepositoryError):
    """Raised when a repository fails to initialize."""

    pass


def is_git_repository(path: pathlib.Path) -> bool:
    """
    Check if a path is a git repository.

    Args:
        path: Path to check for git repository

    Returns:
        bool: True if path contains .git directory or file
    """
    git_path = path / ".git"
    return git_path.exists()


def scan_repositories(root_dir_str: str) -> dict[str, gp.Repository]:
    """
    Recursively scan directory for git repositories and initialize Repository objects.

    Args:
        root_dir_str: Root directory to start scanning from

    Returns:
        Dict mapping repository names to gitpandas Repository objects

    Raises:
        RepositoryInitError: If repository initialization fails
    """
    discovered_repos: dict[str, gp.Repository] = {}
    root_path = pathlib.Path(root_dir_str).expanduser().resolve()
    logger.info(f"Scanning for Git repositories under: {root_path}")

    if not root_path.is_dir():
        logger.warning(f"Scan directory not found or is not a directory: {root_path}")
        return {}

    for dirpath, dirnames, _filenames in os.walk(str(root_path), topdown=True):
        current_path = pathlib.Path(dirpath)

        if is_git_repository(current_path):
            repo_name = current_path.name
            repo_path_str = str(current_path)
            logger.info(f"Found potential repository: '{repo_name}' at {repo_path_str}")

            try:
                repo_obj = gp.Repository(repo_path_str)
                if repo_name in discovered_repos:
                    logger.warning(
                        f"Duplicate repository name '{repo_name}' found. "
                        f"Using '{repo_path_str}' (found last). "
                        f"Previous was '{discovered_repos[repo_name].repo_path}'"
                    )
                discovered_repos[repo_name] = repo_obj
                logger.info(f"Successfully initialized repository: '{repo_name}'")
                dirnames[:] = []  # Don't recurse into discovered repository
                continue
            except Exception as e:
                logger.error(
                    f"Failed to initialize gitpandas.Repository for '{repo_name}' at {repo_path_str}. Error: {e}"
                )
                dirnames[:] = []  # Don't recurse into failed repository
                continue

        # Prune directories to skip
        dirnames[:] = [d for d in dirnames if d not in Config.SKIP_DIRS and not d.startswith(".")]

    logger.info(f"Scan complete. Initialized {len(discovered_repos)} repositories.")
    return discovered_repos


def initialize_repo_cache() -> None:
    """
    Initialize the global repository cache by scanning the filesystem.
    Updates the REPO_CACHE global variable.
    """
    global REPO_CACHE
    REPO_CACHE = scan_repositories(Config.SCAN_ROOT_DIR)
    if not REPO_CACHE:
        logger.warning("No Git repositories successfully initialized from scan directory.")
    else:
        logger.info("Available repository names (initialized):")
        for name in sorted(REPO_CACHE.keys()):
            logger.info(f"- {name}")


# --- MCP Server Setup ---
mcp = FastMCP("GitPandas MCP Server")

# --- Dynamic Tool Registration ---


def serialize_pandas_object(obj: Any) -> Any:
    """
    Serialize pandas and numpy objects to JSON-friendly format.

    Args:
        obj: Object to serialize

    Returns:
        JSON-serializable version of the object
    """
    if isinstance(obj, pd.DataFrame):
        # Handle datetime index/columns
        if pd.api.types.is_datetime64_any_dtype(obj.index):
            obj.index = obj.index.strftime("%Y-%m-%dT%H:%M:%SZ")
            obj = obj.reset_index()

        for col in obj.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "datetimetz"]).columns:
            if col in obj.columns:
                obj[col] = obj[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        return obj.to_dict(orient="records")

    elif isinstance(obj, pd.Series):
        if pd.api.types.is_datetime64_any_dtype(obj.index):
            obj.index = obj.index.strftime("%Y-%m-%dT%H:%M:%SZ")
        return obj.to_dict()

    elif isinstance(obj, np.int64 | np.float64):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj.item()

    elif isinstance(obj, np.bool_):
        return bool(obj)

    elif isinstance(obj, np.ndarray):
        cleaned_list = [x.item() if hasattr(x, "item") else x for x in obj]
        return [None if isinstance(x, float) and (np.isnan(x) or np.isinf(x)) else x for x in cleaned_list]

    elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None

    return obj


def create_repo_tool_wrapper(method_name: str, original_method: Callable) -> Callable:
    """
    Create a wrapper function for a Repository method to expose it as an MCP tool.

    Args:
        method_name: Name of the method to wrap
        original_method: The original Repository method

    Returns:
        Wrapped function that can be registered as an MCP tool
    """
    original_sig = inspect.signature(original_method)
    original_params = list(original_sig.parameters.values())

    @wraps(original_method)
    def wrapper(repo_name: str, *args: Any, **kwargs: Any) -> Any:
        repo_obj = REPO_CACHE.get(repo_name)
        if not repo_obj:
            raise RepositoryNotFoundError(
                f"Repository '{repo_name}' not found. Use 'list_available_repos' to see available names."
            )

        tool_call_sig = f"{wrapper.__name__}(repo_name='{repo_name}'"
        if args:
            tool_call_sig += f", args={args}"
        if kwargs:
            tool_call_sig += f", kwargs={kwargs}"
        tool_call_sig += ")"
        logger.info(f"Executing: {tool_call_sig}")

        try:
            # Handle method calls based on parameter count
            if len(inspect.signature(original_method).parameters) == 1:
                if args or kwargs:
                    logger.warning(
                        f"Method {method_name} only expects 'self', but received "
                        f"extra args/kwargs which will be ignored: args={args}, kwargs={kwargs}"
                    )
                result = original_method(repo_obj)
            else:
                result = original_method(repo_obj, *args, **kwargs)

            return serialize_pandas_object(result)

        except Exception as e:
            logger.error(f"Error calling method {method_name} on repository {repo_name}: {e}")
            raise RuntimeError(f"Failed to execute '{method_name}' on '{repo_name}'. Error: {e}") from e

    # Update wrapper metadata
    wrapper.__name__ = f"repo_{method_name}"
    wrapper.__doc__ = f"""
    Repository Tool: {method_name}
    
    Args:
        repo_name (str): Name of the target repository
    """

    # Add original args description if available
    if original_method.__doc__:
        wrapper.__doc__ += f"\n\nOriginal Documentation:\n{original_method.__doc__}"

    # Set wrapper signature
    try:
        new_params = [inspect.Parameter("repo_name", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=str)]
        if len(original_params) > 1:
            new_params.extend(original_params[1:])
        wrapper.__signature__ = original_sig.replace(parameters=new_params)
    except Exception as e:
        logger.warning(f"Could not set dynamic signature for {method_name}: {e}")

    return wrapper


def register_repository_tools(mcp_instance: FastMCP, repo_class: type) -> None:
    """
    Register public methods of a class as MCP tools.

    Args:
        mcp_instance: FastMCP instance to register tools with
        repo_class: Class whose methods should be registered
    """
    logger.info(f"Dynamically registering tools from class: {repo_class.__name__}")

    # Methods to exclude from registration
    exclusion_list = {
        # Dunder methods
        "__init__",
        "__del__",
        "__repr__",
        "__str__",
        # Private/internal helpers
        "_repo_name",
        "_add_labels_to_df",
        "__check_extension",
        "_commits_per_tags_recursive",
        "_commits_per_tags_helper",
        "_file_last_edit",
        # Properties
        "repo_name",
        # Complex or potentially problematic methods
        "blame",
        "cumulative_blame",
        "parallel_cumulative_blame",
        "_parallel_cumulative_blame_func",
    }

    for name, member in inspect.getmembers(repo_class):
        if inspect.isfunction(member) and not name.startswith("_") and name not in exclusion_list:
            logger.info(f"  - Registering method: {name}")
            try:
                wrapper_func = create_repo_tool_wrapper(name, member)
                mcp_instance.tool()(wrapper_func)
            except Exception as e:
                logger.error(f"    Error registering tool for method '{name}': {e}")


# --- Manual Tools ---
@mcp.tool()
def list_available_repos() -> list[str]:
    """
    List names of successfully initialized Git repositories.

    Returns:
        List of repository names
    """
    if not REPO_CACHE:
        logger.warning("Repository cache is empty. Was the initial scan successful?")
        return []
    return sorted(REPO_CACHE.keys())


def main() -> None:
    """Initialize and start the GitPandas MCP Server."""
    logger.info("Initializing repository cache (this may take a moment)...")
    initialize_repo_cache()

    logger.info("Registering dynamic repository tools...")
    register_repository_tools(mcp, gp.Repository)

    logger.info("Starting GitPandas MCP Server...")
    mcp.run()


if __name__ == "__main__":
    main()
