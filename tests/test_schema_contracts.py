"""Pin the populated result schemas (columns, index levels, dtype kinds) of the core analysis methods.

The contracts live in ``docs/source/schema_contracts.py`` and also generate the "Result schemas" docs
page, so docs, constants and real output are checked against each other here.
"""

import importlib.util
import pathlib

import pytest
from pandas.api import types as pdt

from gitpandas import ProjectDirectory, Repository
from tests.test_cache_parity import EARLY, build_parity_repo
from tests.test_cache_parity import _git as git

DOCS = pathlib.Path(__file__).resolve().parent.parent / "docs" / "source"
_spec = importlib.util.spec_from_file_location("schema_contracts", DOCS / "schema_contracts.py")
contracts = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(contracts)

KIND_CHECKS = {
    "str": pdt.is_string_dtype,
    "int": pdt.is_integer_dtype,
    "float": pdt.is_float_dtype,
    "bool": pdt.is_bool_dtype,
    "datetime": pdt.is_datetime64_any_dtype,
    "object": pdt.is_object_dtype,
    "nullable_str": lambda x: pdt.is_string_dtype(x) or pdt.is_object_dtype(x),
}


@pytest.fixture(scope="module")
def dirs(tmp_path_factory, default_branch):
    root = tmp_path_factory.mktemp("schemas")
    empty = root / "empty"
    empty.mkdir()
    git(empty, "init", "-b", default_branch)
    return (
        [
            build_parity_repo(root / "alpha", default_branch, "alpha_only.txt"),
            build_parity_repo(root / "beta", default_branch, "beta_only.txt"),
        ],
        empty,
    )


def _objects(dirs, branch, layer):
    populated, empty = dirs
    if layer == contracts.REPOSITORY:
        return Repository(str(populated[0]), default_branch=branch), Repository(str(empty), default_branch=branch)
    return (
        ProjectDirectory([str(d) for d in populated], default_branch=branch, verbose=False),
        ProjectDirectory([str(empty)], default_branch=branch, verbose=False),
    )


def _call(obj, schema):
    kwargs = dict(schema.kwargs)
    if schema.method == "commits_in_tags":
        kwargs["start"] = EARLY
    return getattr(obj, schema.method)(**kwargs)


def _id(schema):
    return f"{schema.layer}.{schema.call}"


@pytest.mark.parametrize("schema", contracts.SCHEMAS, ids=_id)
def test_populated_schema(schema, dirs, default_branch):
    populated, _ = _objects(dirs, default_branch, schema.layer)
    df = _call(populated, schema)

    assert len(df) > 0, "contract must be pinned against a populated frame"
    assert list(df.columns) == [name for name, _ in schema.columns]
    for name, kind in schema.columns:
        assert KIND_CHECKS[kind](df[name]), f"{name}: expected {kind}, got {df[name].dtype}"

    assert list(df.index.names) == [name for name, _ in schema.index]
    for level, (name, kind) in enumerate(schema.index):
        values = df.index.get_level_values(level)
        assert KIND_CHECKS[kind](values), f"index {name}: expected {kind}, got {values.dtype}"


@pytest.mark.parametrize(("layer", "method"), list(contracts.EMPTY_SCHEMAS), ids=lambda v: str(v))
def test_empty_schema_differs_where_named(layer, method, dirs, default_branch):
    _, empty = _objects(dirs, default_branch, layer)
    columns, index_names = contracts.EMPTY_SCHEMAS[(layer, method)]
    kwargs = {"start": EARLY} if method == "commits_in_tags" else {}
    df = getattr(empty, method)(**kwargs)

    assert len(df) == 0
    assert list(df.columns) == columns
    assert list(df.index.names) == index_names
    populated = [s for s in contracts.SCHEMAS if (s.layer, s.method) == (layer, method)]
    assert populated
    assert any(
        [n for n, _ in s.columns] != columns or [n for n, _ in s.index] != index_names for s in populated
    ), "EMPTY_SCHEMAS entry equals the populated contract; remove it"


def test_every_contract_is_unique_and_targets_real_methods():
    keys = [(s.layer, s.method, s.kwargs) for s in contracts.SCHEMAS]
    assert len(keys) == len(set(keys))
    for s in contracts.SCHEMAS:
        cls = Repository if s.layer == contracts.REPOSITORY else ProjectDirectory
        assert callable(getattr(cls, s.method))


def test_docs_page_matches_contracts():
    page = (DOCS / "result_schemas.rst").read_text()
    assert page == contracts.render_rst(), (
        "docs/source/result_schemas.rst is stale; run: "
        "python docs/source/schema_contracts.py > docs/source/result_schemas.rst"
    )


def test_docs_page_is_in_toctree():
    assert "\n   result_schemas\n" in (DOCS / "index.rst").read_text()
