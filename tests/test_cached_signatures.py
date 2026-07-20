import importlib.util
import inspect
import sys
import types
from pathlib import Path

from gitpandas import Repository


def test_cached_repository_methods_preserve_signatures():
    assert inspect.signature(Repository.commit_history) == inspect.Signature(
        parameters=[
            inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter("branch", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None),
            inspect.Parameter("limit", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None),
            inspect.Parameter("days", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None),
            inspect.Parameter("ignore_globs", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None),
            inspect.Parameter("include_globs", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None),
        ]
    )
    assert list(inspect.signature(Repository.blame).parameters) == [
        "self",
        "rev",
        "committer",
        "by",
        "ignore_globs",
        "include_globs",
    ]
    assert inspect.signature(Repository.blame).parameters["rev"].default == "HEAD"


def test_mcp_wrapper_preserves_cached_method_signature(monkeypatch):
    class FastMCPStub:
        def __init__(self, _name):
            pass

        def tool(self):
            return lambda func: func

    mcp_module = types.ModuleType("mcp")
    mcp_server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = FastMCPStub
    monkeypatch.setitem(sys.modules, "mcp", mcp_module)
    monkeypatch.setitem(sys.modules, "mcp.server", mcp_server_module)
    monkeypatch.setitem(sys.modules, "mcp.server.fastmcp", fastmcp_module)

    server_path = Path(__file__).parents[1] / "mcp_server" / "server.py"
    spec = importlib.util.spec_from_file_location("gitpandas_mcp_server", server_path)
    server = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(server)

    wrapper = server.create_repo_tool_wrapper("commit_history", Repository.commit_history)

    signature = inspect.signature(wrapper)
    assert list(signature.parameters) == [
        "repo_name",
        "branch",
        "limit",
        "days",
        "ignore_globs",
        "include_globs",
    ]
    assert signature.parameters["repo_name"].annotation is str
    assert all(parameter.kind is not inspect.Parameter.VAR_POSITIONAL for parameter in signature.parameters.values())
    assert all(parameter.kind is not inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())
