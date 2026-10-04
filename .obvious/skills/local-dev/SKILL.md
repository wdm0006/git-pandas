---
name: local-dev
description: Stand up and verify a working local dev environment for git-pandas (uv venv, pytest, ruff, examples, MCP server) — recorded from the 2026-09-06 onboarding run
---

# Local dev onboarding — git-pandas

Durable record of the autobuild onboarding run (2026-09-06 UTC) against `master` @ `e75870c`,
in sandbox `cmp_aPYkTByo`. The snapshot for that sandbox already contains a working
environment — this skill also documents how to rebuild it from a bare checkout.

## What "healthy" means here

git-pandas is a pure Python library. There is no app to serve, no database, no ports.
A healthy dev stack = venv with the right extras + green test suite + clean lint + a
runnable example. Evidence below.

## Fresh setup (from a bare checkout)

```bash
# 1. uv (package manager the repo standardizes on — Makefile + CI both use it)
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# 2. git identity — tests create repos and make commits
git config --global user.email "dev@example.com"
git config --global user.name  "Dev"

# 3. venv on 3.12 (matches the CI matrix; uv downloads its managed CPython)
uv venv --python 3.12

# 4. deps — dev AND all. dev is not optional: joblib + pytest-mock live only there
uv pip install -e ".[dev,all]"

# 5. optional, only if you need the MCP server entry point (make mcp)
uv pip install "mcp[cli]<2" uvicorn
```

## Verification checklist (all must pass)

1. `source .venv/bin/activate`
2. `MPLBACKEND=Agg python -m pytest -m "not slow"` → expect **572 passed, 1 deselected
   (slow), 0 failed** (~160 s; tests marked `remote` fetch from github.com — network required)
3. `python -m ruff check .` → "All checks passed!"
4. `MPLBACKEND=Agg python examples/commit_history.py` → exit 0, prints committer stats and
   file-change frames analyzing the git-pandas repo itself
5. Optional MCP smoke: pipe an `initialize` + `tools/list` JSON-RPC pair into
   `python mcp_server/server.py` and check for a `serverInfo` response

## Known traps (cost real time during onboarding)

- **joblib only in `dev` extra.** Installing `.[all]` (what `make test` does via `setup-all`)
  yields 14 failures in `parallel_cumulative_blame` + bulk-fetch tests, all logging
  "Joblib not installed". Fix: install `.[dev,all]`. CI avoids this by installing `.[dev]`.
- **mcp 2.x breaks the server.** `mcp_server/server.py` imports the v1 API
  (`mcp.server.fastmcp.FastMCP`); mcp 2.x renamed it to `MCPServer`. Pin `mcp[cli]<2`
  (onboarding used 1.29.1). The pytest suite never hits this — `test_mcp_serialization.py`
  stubs the import.
- **`make lint` mutates** (`ruff check --fix --unsafe-fixes`). The CI gate is the read-only
  `python -m ruff check .` — use that unless asked to auto-fix.
- API signatures to not mis-guess (verified): `blame(rev, committer, by, ignore_globs,
  include_globs)` — no `exts` kwarg; `bus_factor(by, ignore_globs, include_globs)` — no
  `limit` kwarg; `ProjectDirectory(working_dir)` — no `max_depth`, and it has no
  `repositories()` attribute.

## Evidence from the onboarding run

- CPython 3.12.14 · pandas 3.0.5 · numpy 2.5.3 · git 2.47.3 · uv 0.12.10 · gitpandas 2.5.0 (editable)
- pytest `-m "not slow"`: **572 passed, 1 deselected, 2 warnings, 0 failed**, exit 0 (~160 s)
- ruff check: all checks passed
- `examples/commit_history.py`: exit 0 — repository + project-directory analyses printed
- Direct API: `commit_history`, `blame(by="repository")`, `bus_factor(by="file")` (120 rows),
  `bus_factor(by="repository")` (2), `hours_estimate` (6 rows), `ProjectDirectory.commit_history`
- MCP: stdio JSON-RPC initialize + tools/list handshake OK with mcp 1.29.1
- Snapshot baked after all checks passed: template `b04vxdj9qvlfbrzgb205:default`
  @ 2026-09-06T20:25:59.276Z
