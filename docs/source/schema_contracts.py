"""Populated result-schema contracts, shared by the tests and the "Result schemas" docs page.

``tests/test_schema_contracts.py`` asserts every entry against real output and that
``result_schemas.rst`` equals ``render_rst()``. Regenerate the page with::

    python docs/source/schema_contracts.py > docs/source/result_schemas.rst

Kinds are portable across pandas 2 and 3: ``str``, ``int``, ``float``, ``bool``, ``datetime``, ``object``;
``nullable_str`` is text that is None for lightweight tags, so pandas 2 may store it as ``object``.
An index name of ``None`` means an unnamed default integer index.
"""

from dataclasses import dataclass

REPOSITORY = "Repository"
PROJECT = "ProjectDirectory"


@dataclass(frozen=True)
class Schema:
    layer: str
    method: str
    kwargs: tuple  # sorted (name, value) pairs
    columns: tuple  # ((name, kind), ...) in order
    index: tuple  # ((name, kind), ...) one pair per index level
    note: str = ""

    @property
    def call(self):
        args = ", ".join(f"{k}={v!r}" for k, v in self.kwargs)
        return f"{self.method}({args})"


def _s(layer, method, columns, index=((None, "int"),), kwargs=(), note=""):
    return Schema(layer, method, tuple(kwargs), tuple(columns), tuple(index), note)


DATE_INDEX = (("date", "datetime"),)
TAG_INDEX = (("tag_date", "datetime"), ("commit_date", "datetime"))

SCHEMAS = [
    _s(
        REPOSITORY,
        "commit_history",
        [
            ("author", "str"),
            ("committer", "str"),
            ("message", "str"),
            ("commit_sha", "str"),
            ("lines", "int"),
            ("insertions", "int"),
            ("deletions", "int"),
            ("net", "int"),
            ("branch", "str"),
            ("repository", "str"),
        ],
        DATE_INDEX,
        note="UTC ``date`` index; includes a ``branch`` column.",
    ),
    _s(
        PROJECT,
        "commit_history",
        [
            ("repository", "str"),
            ("author", "str"),
            ("committer", "str"),
            ("date", "datetime"),
            ("message", "str"),
            ("commit_sha", "str"),
            ("lines", "int"),
            ("insertions", "int"),
            ("deletions", "int"),
            ("net", "int"),
        ],
        note="``date`` is a column; the integer index restarts at 0 for each repository; no ``branch`` column.",
    ),
    _s(
        REPOSITORY,
        "file_change_history",
        [
            ("filename", "str"),
            ("insertions", "int"),
            ("deletions", "int"),
            ("lines", "int"),
            ("message", "str"),
            ("committer", "str"),
            ("author", "str"),
            ("date", "datetime"),
            ("repository", "str"),
        ],
        DATE_INDEX,
        note="``date`` is both the index and a column.",
    ),
    _s(
        PROJECT,
        "file_change_history",
        [
            ("repository", "str"),
            ("date", "datetime"),
            ("author", "str"),
            ("committer", "str"),
            ("message", "str"),
            ("filename", "str"),
            ("insertions", "int"),
            ("deletions", "int"),
        ],
        note="No ``lines`` column; ``date`` is a column on a default integer index.",
    ),
    _s(
        REPOSITORY,
        "revs",
        [("date", "int"), ("rev", "str"), ("repository", "str")],
        note="``date`` is epoch seconds stored as an integer column.",
    ),
    _s(
        PROJECT,
        "revs",
        [("repository", "str"), ("rev", "str"), ("date", "float")],
        note="``date`` is epoch seconds stored as a float column.",
    ),
    _s(
        REPOSITORY,
        "blame",
        [("loc", "int"), ("repository", "str")],
        (("committer", "str"),),
        kwargs=(("by", "repository"),),
    ),
    _s(
        PROJECT,
        "blame",
        [("loc", "int")],
        (("committer", "str"),),
        kwargs=(("by", "repository"),),
        note="No ``repository`` column.",
    ),
    _s(
        REPOSITORY,
        "blame",
        [("loc", "int"), ("repository", "str")],
        (("committer", "str"), ("file", "str")),
        kwargs=(("by", "file"),),
    ),
    _s(
        PROJECT,
        "blame",
        [("loc", "int")],
        (("committer", "str"), ("file", "str"), ("repository", "str")),
        kwargs=(("by", "file"),),
        note="``repository`` is an index level, not a column.",
    ),
    _s(
        REPOSITORY,
        "tags",
        [
            ("tag", "str"),
            ("annotated", "bool"),
            ("annotation", "nullable_str"),
            ("tag_sha", "nullable_str"),
            ("commit_sha", "str"),
            ("repository", "str"),
        ],
        TAG_INDEX,
    ),
    _s(
        PROJECT,
        "tags",
        [
            ("tag", "str"),
            ("annotated", "bool"),
            ("annotation", "nullable_str"),
            ("tag_sha", "nullable_str"),
            ("commit_sha", "str"),
            ("repository", "str"),
        ],
        TAG_INDEX,
    ),
    _s(
        REPOSITORY,
        "commits_in_tags",
        [("commit_sha", "str"), ("tag", "str"), ("repository", "str")],
        TAG_INDEX,
    ),
    _s(
        PROJECT,
        "commits_in_tags",
        [("commit_sha", "str"), ("tag", "str"), ("repository", "str")],
        TAG_INDEX,
    ),
    _s(
        REPOSITORY,
        "punchcard",
        [
            ("hour_of_day", "int"),
            ("day_of_week", "int"),
            ("lines", "int"),
            ("insertions", "int"),
            ("deletions", "int"),
            ("net", "int"),
        ],
    ),
    _s(
        PROJECT,
        "punchcard",
        [
            ("hour_of_day", "int"),
            ("day_of_week", "int"),
            ("lines", "int"),
            ("insertions", "int"),
            ("deletions", "int"),
            ("net", "int"),
        ],
    ),
    _s(
        REPOSITORY,
        "hours_estimate",
        [("committer", "str"), ("hours", "float"), ("repository", "str")],
    ),
    _s(
        PROJECT,
        "hours_estimate",
        [("committer", "str"), ("hours", "float"), ("repository", "str")],
    ),
    _s(
        REPOSITORY,
        "file_detail",
        [
            ("file", "str"),
            ("loc", "int"),
            ("file_owner", "str"),
            ("ext", "str"),
            ("last_edit_date", "datetime"),
            ("repository", "str"),
        ],
        note="``file`` is a column.",
    ),
    _s(
        PROJECT,
        "file_detail",
        [
            ("index", "int"),
            ("loc", "int"),
            ("file_owner", "str"),
            ("ext", "str"),
            ("last_edit_date", "datetime"),
        ],
        (("file", "str"), ("repository", "str")),
        note="``(file, repository)`` index; a stray ``index`` column of row numbers leaks through.",
    ),
    _s(
        REPOSITORY,
        "bus_factor",
        [("repository", "str"), ("bus factor", "int")],
        kwargs=(("by", "repository"),),
    ),
    _s(
        PROJECT,
        "bus_factor",
        [("projectd", "str"), ("bus factor", "int")],
        kwargs=(("by", "projectd"),),
        note="Default grouping; the first column is named ``projectd``.",
    ),
    _s(
        REPOSITORY,
        "bus_factor",
        [("file", "str"), ("bus factor", "int"), ("repository", "str")],
        kwargs=(("by", "file"),),
    ),
    _s(
        PROJECT,
        "bus_factor",
        [("file", "str"), ("bus factor", "int"), ("repository", "str")],
        kwargs=(("by", "file"),),
    ),
    _s(
        PROJECT,
        "bus_factor",
        [("repository", "str"), ("bus factor", "object")],
        kwargs=(("by", "repository"),),
        note="``bus factor`` has object dtype.",
    ),
]

# Where the empty frame legitimately differs from the populated one: (layer, method) -> (columns, index names).
EMPTY_SCHEMAS = {
    (REPOSITORY, "file_change_history"): (
        ["filename", "insertions", "deletions", "lines", "message", "committer", "author"],
        [None],
    ),
    (REPOSITORY, "revs"): (["date", "rev"], [None]),
    (REPOSITORY, "tags"): (
        ["tag_date", "commit_date", "tag", "annotated", "annotation", "tag_sha", "commit_sha", "repository"],
        [None],
    ),
    (REPOSITORY, "commits_in_tags"): (["commit_sha", "tag", "tag_date", "commit_date"], [None]),
    (PROJECT, "revs"): (["repository", "rev"], [None]),
    (PROJECT, "tags"): ([], [None]),
    (PROJECT, "commits_in_tags"): (["commit_sha", "tag", "tag_date", "commit_date"], [None]),
}


def _levels(pairs):
    return ", ".join(f"``{name or '(unnamed integer index)'}`` ({kind})" for name, kind in pairs)


def render_rst():
    out = [
        ".. This page is generated: python docs/source/schema_contracts.py > docs/source/result_schemas.rst",
        "",
        "Result schemas",
        "==============",
        "",
        "Exact column order, index levels and dtype kinds of populated results, pinned by",
        "``tests/test_schema_contracts.py``. Kinds are ``str``, ``int``, ``float``, ``bool`` and ``datetime``",
        "(UTC); they hold on pandas 2 and 3. Shapes differ between ``Repository`` and ``ProjectDirectory`` and",
        "between populated and empty results; both are listed as they are today.",
        "",
    ]
    for schema in SCHEMAS:
        title = f"{schema.layer}.{schema.call}"
        out += [title, "-" * len(title), "", f"Index: {_levels(schema.index)}", ""]
        if schema.note:
            out += [schema.note, ""]
        out += [".. list-table::", "   :header-rows: 1", "", "   * - Column", "     - Kind"]
        for name, kind in schema.columns:
            out += [f"   * - ``{name}``", f"     - {kind}"]
        out.append("")
    out += ["Empty results", "-------------", "", "Where the empty frame differs from the populated one:", ""]
    out += [".. list-table::", "   :header-rows: 1", "", "   * - Method", "     - Columns", "     - Index"]
    for (layer, method), (columns, index) in EMPTY_SCHEMAS.items():
        cols = ", ".join(f"``{c}``" for c in columns) or "(none)"
        idx = ", ".join(f"``{i}``" if i else "(unnamed integer index)" for i in index)
        out += [f"   * - {layer}.{method}", f"     - {cols}", f"     - {idx}"]
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    print(render_rst(), end="")
