import importlib.util
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_import_gitpandas_does_not_require_mcp():
    code = "import sys; sys.modules['mcp'] = None; import gitpandas"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT)
    assert result.returncode == 0, result.stderr


def test_missing_extra_names_install_command():
    code = (
        "import sys; sys.modules['mcp'] = None; sys.modules['mcp.server'] = None; "
        "sys.modules['mcp.server.fastmcp'] = None\n"
        "try:\n    import gitpandas.mcp_server\nexcept ImportError as e:\n    print(e)\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT)
    assert "git-pandas[mcp]" in result.stdout, result.stdout + result.stderr


def test_pyproject_declares_extra_and_console_script():
    meta = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    assert meta["scripts"]["git-pandas-mcp"] == "gitpandas.mcp_server:main"
    extras = meta["optional-dependencies"]
    for name in ("mcp", "all", "dev"):
        assert "mcp[cli]<2" in extras[name]


def test_shim_pep723_requires_mcp_extra():
    text = (ROOT / "mcp_server" / "server.py").read_text()
    block = text.split("# ///")[1]
    assert "git-pandas[mcp]" in block
    assert importlib.util.find_spec("gitpandas") is not None
