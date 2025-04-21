# GitPandas MCP Server

This directory contains an experimental Model Context Protocol (MCP) server that acts as a wrapper around the `git-pandas` Python library. It allows MCP-compatible clients (like AI assistants or IDE extensions) to interact with git repository data using structured tools.

## Prerequisites

*   Python 3.8+
*   `uv` (Python package installer and virtual environment manager). See [uv installation guide](https://github.com/astral-sh/uv#installation). You can often install it via pip: `pip install uv`.

## Setup and Installation (using uv)

1.  **Clone the Repository:** If you haven't already, clone the main `git-pandas` repository.

## Connecting with Claude Desktop (or similar clients)

1.  Open Claude Desktop's settings or preferences.
2.  Look for a section related to "Tools", "MCP Servers", "External Tools", or similar.
3.  Add a new server configuration.
4.  When prompted for the server command or path, paste the **full `uv run` command** you constructed in the previous section (e.g., `uv run --cwd /absolute/path/to/git-pandas/mcp_server python server.py`).
5.  Save the configuration.

Claude Desktop should now be able to launch this server process using `uv` when needed and communicate with it to use the defined tools.