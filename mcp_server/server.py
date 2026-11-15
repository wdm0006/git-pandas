# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "git-pandas[mcp]>=2.7.1",
#   "uvicorn",
# ]
# ///
"""Thin launcher for the packaged server (``gitpandas.mcp_server``).

Prefers the checkout's own ``gitpandas`` when run from a clone, so the script
and the library never come from different versions.
"""

import pathlib
import sys

_root = pathlib.Path(__file__).resolve().parents[1]
if (_root / "gitpandas" / "mcp_server.py").exists():
    sys.path.insert(0, str(_root))

from gitpandas.mcp_server import main  # noqa: E402

if __name__ == "__main__":
    main()
