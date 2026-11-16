# GitPandas MCP Server

An experimental Model Context Protocol (MCP) stdio server wrapping the `git-pandas` library, so MCP clients (AI assistants, IDE extensions) can query git repository data. The implementation lives in the package as `gitpandas.mcp_server`.

## Install and launch

```bash
pip install "git-pandas[mcp]"
git-pandas-mcp
```

Without the `mcp` extra, `import gitpandas` works as before; importing the server raises an error telling you to install `git-pandas[mcp]`.

From a clone, `uv run mcp_server/server.py` (or `make mcp`) still works; the script is a thin launcher that uses the checkout's own `gitpandas`.

On startup the server scans `~/Documents` for git repositories (up to 1,000) and registers `list_available_repos` plus a `repo_<method>` tool for each eligible `Repository` method (`blame`, `cumulative_blame` and `parallel_cumulative_blame` are excluded).

## Connecting with Claude Desktop (or similar clients)

Add an MCP server entry whose command is the installed script:

```json
{
  "mcpServers": {
    "git-pandas": { "command": "git-pandas-mcp" }
  }
}
```

Or, without a persistent install: `uvx --from "git-pandas[mcp]" git-pandas-mcp`.
