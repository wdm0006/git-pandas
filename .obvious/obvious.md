# git-pandas — Agent Guide

**Repo:** `wdm0006/git-pandas` · **Default branch:** `master` · **Language:** Python (library, v2.5.0)

git-pandas turns Git repository data into pandas DataFrames — a pure Python library with an
optional MCP stdio server. **No web app, no database, no listening ports, no long-running
services.** "Local dev is healthy" means: venv installed, test suite green, lint clean,
examples run.

## Stack

- **Runtime:** Python ≥ 3.10 (CI matrix: 3.10 / 3.11 / 3.12 × pandas 2.x / 3.x)
- **Package manager:** uv (hatchling build backend; `uv.lock` is gitignored — resolution is fresh from `pyproject.toml`)
- **Core deps:** gitpython, numpy, pandas (≥2.0), requests
- **Tooling:** pytest (+ pytest-cov, pytest-mock), ruff (lint + format), sphinx, coverage
- **Parallelism:** joblib — ⚠️ only in the `dev` extra, **not** in `all`
- **Optional:** redis (RedisDFCache — its tests are fully mocked, no server needed), matplotlib / lifelines (examples)
- **MCP server:** `mcp[cli]<2` + uvicorn — in no extras group; declared only in the PEP 723 block of `mcp_server/server.py`

## Environment (baked into the sandbox snapshot)

- `.venv/` at repo root: uv venv on CPython 3.12.14 (uv-managed), git-pandas 2.5.0 installed
  editable with extras `dev` + `all`, plus `mcp[cli]<2` (1.29.1) and uvicorn
- uv 0.12.10 at `~/.local/bin` (add to PATH: `export PATH="$HOME/.local/bin:$PATH"`)
- git identity configured globally — required, tests create repos and make commits
- Activate: `source .venv/bin/activate` (or prefix commands with `uv run`)

## Commands

| Task | Command |
|---|---|
| Fresh install | `uv venv --python 3.12 && uv pip install -e ".[dev,all]"` |
| Tests (CI parity) | `MPLBACKEND=Agg python -m pytest -m "not slow"` |
| Tests with coverage (Makefile) | `make test` (reinstalls `.[all]` first — see gotcha #1) |
| Single test | `make test-single test=tests/test_Repository/test_properties.py` |
| Lint (read-only, CI gate) | `python -m ruff check .` |
| Lint (⚠️ mutates files) | `make lint` — `--fix --unsafe-fixes`; run only when asked |
| Format | `make format` |
| Run an example | `make run-example example=commit_history` |
| MCP server (stdio) | `make mcp` → `python mcp_server/server.py` |
| Docs build | `make docs` (CI also installs `docs/requirements.txt` first) |

## Gotchas

1. **joblib is only in the `dev` extra.** `uv pip install -e ".[all]"` alone → 14 test failures
   in `parallel_cumulative_blame` / bulk-fetch tests ("Joblib not installed"). CI installs
   `.[dev]`; the Makefile's `make test` path installs `.[all]`. Install `.[dev,all]` to cover both.
2. **`make lint` auto-fixes** and can rewrite source files. Use `python -m ruff check .` for a
   read-only gate (that's what CI runs).
3. **MCP server needs mcp v1 API** (`from mcp.server.fastmcp import FastMCP`). mcp 2.x renamed
   FastMCP → MCPServer and breaks the import. Pin `mcp[cli]<2`.
4. MCP server scans `~/Documents` by default (`Config.SCAN_ROOT_DIR`, hardcoded — not env-configurable).
5. Redis tests are mocked (`@patch` on `gitpandas.cache`) — no Redis server is needed; the
   `redis` pytest marker is informational.
6. Tests marked `remote` fetch from github.com — network required. `slow` (example smoke tests)
   is deselected by the default `-m "not slow"`.
7. `make test-all` has a typo (`MPLBACKEmND=Agg`) — harmless, but the env var isn't set.
8. The Makefile `gitnoc` target points at a `gitnoc/` directory that does not exist in this repo.
9. `.gitignore` ignores `uv.lock` and `scratch`; `.venv`/`.pytest_cache`/`.ruff_cache` are kept
   out of git via `.git/info/exclude` in the sandbox, not via `.gitignore`.

## Environment variables

None required for tests, lint, or examples. `GitHubProfile` (in `gitpandas/project.py`) queries
the public GitHub REST API over the network via `requests` — no token is read by the library.

## Codebase map

See [`codebase-map.md`](codebase-map.md).

## Local verification

The onboarding run's evidence is in the **Local Verification Summary** below and in
[`skills/local-dev/SKILL.md`](skills/local-dev/SKILL.md).

## Sandbox snapshot

- **Template/snapshot ID:** `b04vxdj9qvlfbrzgb205:default`
- **Captured:** 2026-09-06T20:25:59.276Z (UTC), from live session `izj5y29hgyt91gu7jjefe`
- **Contains:** everything in "Environment" above — uv 0.12.10, uv-managed CPython 3.12.14,
  repo at `/home/user/work/git-pandas` with the populated `.venv`, global git identity

## Local Verification Summary

**Run:** autobuild onboarding, 2026-09-06 (UTC), against `master` @ `e75870c` ("Preserve project revision limit remainder (#87)")
**Stack:** CPython 3.12.14 · pandas 3.0.5 · numpy 2.5.3 · git 2.47.3 · uv 0.12.10

| Check | Result |
|---|---|
| `MPLBACKEND=Agg python -m pytest -m "not slow"` | ✅ **572 passed**, 1 deselected (slow), 2 warnings, 0 failed — exit 0, ~160 s |
| `python -m ruff check .` | ✅ All checks passed |
| `make run-example example=commit_history` | ✅ exit 0 — committer stats, file-change and per-extension frames printed |
| Library API (`Repository`) | ✅ `commit_history` (5 rows), `blame(by="repository")` (5 committers), `bus_factor(by="file")` (120 rows), `bus_factor(by="repository")` (bus factor 2), `hours_estimate` (6 rows) |
| Library API (`ProjectDirectory`) | ✅ `commit_history` (3 rows, `repository` column populated) |
| MCP server | ✅ stdio JSON-RPC `initialize` + `tools/list` handshake OK (serverInfo: GitPandas MCP Server; tools incl. `list_available_repos`, `repo_branches`) |

Dev stack healthy: **yes**. No services to keep alive — the venv *is* the dev stack.
