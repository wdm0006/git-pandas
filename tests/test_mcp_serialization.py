import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture(scope="module")
def server():
    """Import the standalone MCP server with only its FastMCP dependency stubbed."""

    class FastMCPStub:
        def __init__(self, _name):
            pass

        def tool(self):
            return lambda func: func

    mcp_module = types.ModuleType("mcp")
    mcp_server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = FastMCPStub

    saved = {name: sys.modules.get(name) for name in ("mcp", "mcp.server", "mcp.server.fastmcp")}
    sys.modules["mcp"] = mcp_module
    sys.modules["mcp.server"] = mcp_server_module
    sys.modules["mcp.server.fastmcp"] = fastmcp_module
    try:
        server_path = Path(__file__).parents[1] / "mcp_server" / "server.py"
        spec = importlib.util.spec_from_file_location("gitpandas_mcp_server", server_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def _blame_frame():
    """Shaped like Repository.blame(by='repository')."""
    return (
        pd.DataFrame([["Ada", 7], ["Ada", 3], ["Grace", 5]], columns=["committer", "loc"])
        .groupby("committer")["loc"]
        .sum()
        .to_frame()
    )


def _file_blame_frame():
    """Shaped like Repository.blame(by='file')."""
    return (
        pd.DataFrame(
            [["Ada", 7, "a.py"], ["Ada", 3, "b.py"], ["Grace", 5, "a.py"]],
            columns=["committer", "loc", "file"],
        )
        .groupby(["committer", "file"])["loc"]
        .sum()
        .to_frame()
    )


def test_named_index_is_serialized_as_a_column(server):
    records = server.serialize_pandas_object(_blame_frame())

    assert records == [
        {"committer": "Ada", "loc": 10},
        {"committer": "Grace", "loc": 5},
    ]


def test_named_multiindex_levels_are_serialized_as_columns(server):
    records = server.serialize_pandas_object(_file_blame_frame())

    assert sorted(records, key=lambda r: (r["committer"], r["file"])) == [
        {"committer": "Ada", "file": "a.py", "loc": 7},
        {"committer": "Ada", "file": "b.py", "loc": 3},
        {"committer": "Grace", "file": "a.py", "loc": 5},
    ]


def test_datetime_index_and_columns_are_json_safe(server):
    df = pd.DataFrame(
        {
            "committer": ["Ada", "Grace"],
            "last_edit_date": pd.to_datetime(["2024-01-02 03:04:05", "2024-02-03 04:05:06"]),
        },
        index=pd.DatetimeIndex(["2024-01-01 00:00:00", "2024-01-02 12:30:00"], name="date"),
    )

    records = server.serialize_pandas_object(df)

    assert records == [
        {"date": "2024-01-01T00:00:00Z", "committer": "Ada", "last_edit_date": "2024-01-02T03:04:05Z"},
        {"date": "2024-01-02T12:30:00Z", "committer": "Grace", "last_edit_date": "2024-02-03T04:05:06Z"},
    ]


def test_unnamed_datetime_index_is_still_emitted(server):
    df = pd.DataFrame(
        {"loc": [1, 2]},
        index=pd.DatetimeIndex(["2024-01-01 00:00:00", "2024-01-02 12:30:00"]),
    )

    records = server.serialize_pandas_object(df)

    assert records == [
        {"index": "2024-01-01T00:00:00Z", "loc": 1},
        {"index": "2024-01-02T12:30:00Z", "loc": 2},
    ]


def test_datetime_multiindex_levels_are_json_safe(server):
    """Shaped like Repository.commits_in_tags()."""
    df = pd.DataFrame({"commit_sha": ["abc123"], "tag": ["v1.0.0"]}).set_index(
        [
            pd.DatetimeIndex(["2024-01-01 00:00:00"], name="tag_date"),
            pd.DatetimeIndex(["2023-12-31 23:00:00"], name="commit_date"),
        ]
    )

    records = server.serialize_pandas_object(df)

    assert records == [
        {
            "tag_date": "2024-01-01T00:00:00Z",
            "commit_date": "2023-12-31T23:00:00Z",
            "commit_sha": "abc123",
            "tag": "v1.0.0",
        }
    ]


def test_unnamed_range_index_is_not_emitted(server):
    df = pd.DataFrame({"committer": ["Ada"], "loc": [10]})

    assert server.serialize_pandas_object(df) == [{"committer": "Ada", "loc": 10}]


def test_index_level_duplicating_a_column_is_not_reinserted(server):
    """Shaped like Repository.file_change_history(), which keeps 'date' as index and column."""
    df = pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01 00:00:00"]), "filename": ["a.py"]},
    ).set_index(pd.DatetimeIndex(["2024-01-01 00:00:00"], name="date"))

    records = server.serialize_pandas_object(df)

    assert records == [{"date": "2024-01-01T00:00:00Z", "filename": "a.py"}]


def test_serialization_does_not_mutate_the_input_frame(server):
    df = pd.DataFrame(
        {"loc": [1, 2], "last_edit_date": pd.to_datetime(["2024-01-02 03:04:05", "2024-02-03 04:05:06"])},
        index=pd.DatetimeIndex(["2024-01-01 00:00:00", "2024-01-02 12:30:00"], name="date"),
    )
    before = df.copy(deep=True)

    server.serialize_pandas_object(df)

    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.names == before.index.names
    assert list(df.columns) == list(before.columns)
    assert df.index.equals(before.index)
    assert pd.api.types.is_datetime64_any_dtype(df["last_edit_date"])
    pd.testing.assert_frame_equal(df, before)


def test_serialization_does_not_mutate_an_index_bearing_frame(server):
    df = _file_blame_frame()
    before = df.copy(deep=True)

    server.serialize_pandas_object(df)

    assert isinstance(df.index, pd.MultiIndex)
    assert list(df.columns) == ["loc"]
    pd.testing.assert_frame_equal(df, before)


def test_series_with_datetime_index_is_not_mutated(server):
    series = pd.Series([1, 2], index=pd.DatetimeIndex(["2024-01-01 00:00:00", "2024-01-02 12:30:00"]))

    result = server.serialize_pandas_object(series)

    assert result == {"2024-01-01T00:00:00Z": 1, "2024-01-02T12:30:00Z": 2}
    assert isinstance(series.index, pd.DatetimeIndex)
