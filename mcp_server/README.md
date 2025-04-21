# GitPandas MCP Server

This directory contains a Model Context Protocol (MCP) server that acts as a wrapper around the `git-pandas` Python library. It allows MCP-compatible clients (like AI assistants or IDE extensions) to interact with git repository data using structured tools.

## Prerequisites

*   Python 3.8+
*   `uv` (Python package installer and virtual environment manager). See [uv installation guide](https://github.com/astral-sh/uv#installation). You can often install it via pip: `pip install uv`.

## Setup and Installation (using uv)

1.  **Clone the Repository:** If you haven't already, clone the main `git-pandas` repository.
2.  **Navigate to the MCP Server Directory:**
    ```bash
    cd /path/to/your/git-pandas/mcp_server 
    # Replace /path/to/your/git-pandas with the actual path
    ```
3.  **Install Dependencies (using uv script header):** The `server.py` script now includes its dependencies in the header. When you run it with `uv run`, `uv` will automatically handle creating an environment and installing them if needed.
    *Optional: If you still want to pre-install into a persistent virtual environment:* Run this command within the `mcp_server/` directory:
    ```bash
    # This reads dependencies from the server.py header 
    # and installs them into a .venv directory.
    uv pip install -r server.py 
    ```
    *Note: You do **not** need to manually activate the virtual environment when using `uv run`.*

## Running the Server (for Client Configuration using uv)

This server is designed to be **launched directly by the MCP client** (e.g., Claude Desktop) using `uv run`. This avoids needing to manually find the Python executable path within the virtual environment.

The client needs a command that tells `uv` to run the server script *within the correct directory context*.

1.  **Find the absolute path to the `mcp_server/` directory:** Navigate to the directory in your terminal and run:
    ```bash
    # Linux/macOS
    pwd 
    # Windows (Command Prompt)
    cd 
    # Windows (PowerShell)
    Get-Location
    ```
    Copy the full path displayed (e.g., `/Users/you/projects/git-pandas/mcp_server`).

2.  **Construct the Full Command for the Client:**
    Replace `/absolute/path/to/git-pandas/mcp_server` in the following command with the actual path you found:
    ```bash
    uv run --cwd /absolute/path/to/git-pandas/mcp_server python server.py
    ```
    *   `uv run`: Executes a command within the `uv`-managed environment specified in `server.py`.
    *   `--cwd /absolute/path/to/...`: Tells `uv` to change to this directory before running the command. This ensures `server.py` is found and runs with the correct working directory.
    *   `python server.py`: The command to execute using the Python interpreter within the environment.

## Connecting with Claude Desktop (or similar clients)

1.  Open Claude Desktop's settings or preferences.
2.  Look for a section related to "Tools", "MCP Servers", "External Tools", or similar.
3.  Add a new server configuration.
4.  When prompted for the server command or path, paste the **full `uv run` command** you constructed in the previous section (e.g., `uv run --cwd /absolute/path/to/git-pandas/mcp_server python server.py`).
5.  Save the configuration.

Claude Desktop should now be able to launch this server process using `uv` when needed and communicate with it to use the defined tools.

## Available Tools

*   **`list_available_repos() -> List[str]`**
    *   Description: Lists the names of Git repositories under `~/Documents` that were successfully scanned and initialized on server startup.
    *   Arguments: None.
    *   Returns: A sorted list of repository names (strings).
      Example: `["git-pandas", "my-project"]`

*   **`list_branches(repo_name: str) -> List[str]`**
    *   Description: Lists the branches for a specified, previously discovered and initialized repository.
    *   Arguments:
        *   `repo_name` (string, required): The name of the repository (must be one of the names returned by `list_available_repos`).
    *   Returns: A list of branch names as strings.
    *   Behavior: Looks up the provided `repo_name` in the server's cache of initialized repositories. If the name is not found or the repository object is invalid, an error is raised. 