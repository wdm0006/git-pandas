# git-pandas — Codebase Map

Folder-level overview (depth 2). `wdm0006/git-pandas`, default branch `master`, library v2.5.0.

| Path | What it is |
|---|---|
| `gitpandas/` | The library package. `repository.py` (~3.1k lines) — `Repository` class: single-repo metrics (commit history, blame, cumulative blame, bus factor, hours estimate, rev-to-rev, cache warming, remote fetch). `project.py` (~1.6k lines) — `ProjectDirectory` (multi-repo discovery + aggregation) and `GitHubProfile`. `cache.py` (~860 lines) — `EphemeralCache`, `DiskCache`, `RedisDFCache` + the `_string_handle_to_df` memoization decorator every metric goes through. `logging.py` — verbose-mode logger. |
| `gitpandas/utilities/` | `plotting.py` — chart helpers (punchcard, lifeline, cumulative blame); `check_api.py` — API-parity checker (CONTRIBUTING asks for Repository/ProjectDirectory parity). |
| `tests/` | pytest suite. `test_Repository/`, `test_Project/`, `test_utilities/` mirror the package; root-level files cover cache behavior (immutability, threading, warming, key consistency, management), remote fetch, MCP serialization (mocks the `mcp` import — runs without mcp installed), logging, examples smoke (`test_examples.py`, marked `slow`). `conftest.py` builds throwaway git repos in tmp dirs. |
| `examples/` | One runnable script per analysis (commit_history, bus_analysis, cumulative_blame, punchcard, …). Run from repo root: `make run-example example=<name>`. `definitions.py` locates the git-pandas checkout for self-analysis. |
| `mcp_server/` | FastMCP **stdio** server exposing gitpandas metrics as MCP tools. Own README. Needs `mcp[cli]<2` + uvicorn (PEP 723 block, not in extras). |
| `docs/` | Sphinx docs with their own `Makefile` and `requirements.txt`. |
| `.github/workflows/` | `test-suite.yml` — CI matrix py 3.10–3.12 × pandas 2/3, runs `pytest -m "not slow"` + `ruff check`; `test-docs-build.yml` — PR docs check; `docs.yml` — publishes docs from master; `pypi-publish.yml` — sdist to PyPI on release. |
| `.cursor/rules/` | Agent/editor guidance: python, pytest, sphinx-docs, testing standards + a project overview. |
| `img/`, `examples/img/` | Images used by README and examples. |

**Key invariants** (from CONTRIBUTING.md): maintain feature/API parity between `Repository` and
`ProjectDirectory`; sphinx-format docstrings; memory-heavy functions take a `limit` option.
