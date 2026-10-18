"""Value-level parity between cached and uncached results for every public analysis method.

Cache bugs in this library change RESULTS, not just speed (colliding keys, cached frames mutated
in place by derived methods). This module asserts the general invariant instead of one bug at a
time:

* an uncached call equals the first (cache-miss) call on a cached instance,
* the first call equals the second (cache-hit) call,
* running every method in a fixed order (so derived methods such as ``punchcard``,
  ``hours_estimate`` and ``cumulative_blame`` interleave with the methods they read) leaves every
  earlier result unchanged, compared against COPIED snapshots.

``REPOSITORY_CASES`` / ``PROJECT_CASES`` enumerate each public method with representative kwargs;
``EXCLUDED`` lists what is deliberately not compared. ``test_every_public_method_is_listed``
fails when a new public method is added to either class without an entry in one of them.
"""

import copy
import os
import subprocess

import pandas as pd
import pytest

from gitpandas import ProjectDirectory, Repository
from gitpandas.cache import EphemeralCache

DEFAULT = object()  # replaced with the session's default branch name at call time
FEATURE = "feature"
EARLY = pd.Timestamp("2000-01-01", tz="UTC")

# (name, committer name, author name, committer epoch, author epoch offset in seconds, file, lines)
HOUR = 3600
T0 = 1_700_000_000
COMMITS = [
    ("Carol", "Alice", T0, 0, "README.md", 3),
    ("Dave", "Bob", T0 + 20 * 60, -5 * HOUR, "src/app.py", 4),
    ("Carol", "Bob", T0 + 30 * 60, -6 * HOUR, "docs/guide.md", 2),
    ("Dave", "Alice", T0 + 40 * 60, -9 * HOUR, "README.md", 6),
    ("Carol", "Alice", T0 + 100 * HOUR, -2 * HOUR, "src/app.py", 9),
]
FEATURE_COMMIT = ("Dave", "Bob", T0 + 130 * HOUR, -1 * HOUR, "docs/guide.md", 5)


def _git(repo_dir, *args, env=None):
    base = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull}
    subprocess.run(["git", *args], cwd=repo_dir, env={**base, **(env or {})}, check=True, capture_output=True)


def _commit(repo_dir, spec, unique_file, index):
    committer, author, epoch, author_offset, filename, lines = spec
    target = repo_dir / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    # repo-specific line counts so per-file project aggregation can't pass by accident
    target.write_text("".join(f"{repo_dir.name} line {n}\n" for n in range(1, lines + 1)))
    if index == 2:
        (repo_dir / unique_file).write_text("unique\n")
    _git(repo_dir, "add", "-A")
    env = {
        "GIT_COMMITTER_NAME": committer,
        "GIT_COMMITTER_EMAIL": f"{committer.lower()}@example.com",
        "GIT_COMMITTER_DATE": f"{epoch} +0000",
        "GIT_AUTHOR_NAME": author,
        "GIT_AUTHOR_EMAIL": f"{author.lower()}@example.com",
        "GIT_AUTHOR_DATE": f"{epoch + author_offset} +0000",
    }
    _git(repo_dir, "commit", "-m", f"commit {index} in {repo_dir.name}", env=env)


def build_parity_repo(repo_dir, default_branch, unique_file):
    """Pinned-timestamp repo: author != committer, author dates != committer dates, two tags, a feature branch."""
    repo_dir.mkdir(parents=True)
    _git(repo_dir, "init", "-b", default_branch)
    for index, spec in enumerate(COMMITS):
        _commit(repo_dir, spec, unique_file, index)
        if index == 1:
            _git(repo_dir, "tag", "-a", "v1.0", "-m", "first release", env={"GIT_COMMITTER_DATE": f"{spec[2]} +0000"})
        if index == 3:
            _git(repo_dir, "tag", "v2.0")
    _git(repo_dir, "checkout", "-b", FEATURE)
    _commit(repo_dir, FEATURE_COMMIT, unique_file, 99)
    _git(repo_dir, "checkout", default_branch)
    return repo_dir


@pytest.fixture(scope="module")
def parity_dirs(tmp_path_factory, default_branch):
    root = tmp_path_factory.mktemp("parity")
    return [
        build_parity_repo(root / "alpha", default_branch, "alpha_only.txt"),
        build_parity_repo(root / "beta", default_branch, "beta_only.txt"),
    ]


# --- method tables -------------------------------------------------------------------------------
# Callable-style entries are (method name, kwargs). DEFAULT stands for the default branch name.

COMMON_ACTIVITY = [
    ("commit_history", {}),
    ("commit_history", {"branch": DEFAULT}),
    ("commit_history", {"branch": FEATURE}),
    ("commit_history", {"branch": DEFAULT, "date_source": "author"}),
    ("commit_history", {"branch": DEFAULT, "limit": 3, "days": 100000}),
    ("commit_history", {"branch": DEFAULT, "include_globs": ["*.md"]}),
    ("commit_history", {"branch": DEFAULT, "ignore_globs": ["src/*"]}),
    ("file_change_history", {"branch": DEFAULT}),
    ("file_change_history", {"branch": FEATURE, "limit": 4}),
    ("file_change_history", {"branch": DEFAULT, "include_globs": ["*.md"], "ignore_globs": ["docs/*"]}),
    ("file_change_rates", {"branch": DEFAULT}),
    ("file_change_rates", {"branch": FEATURE, "ignore_globs": ["*.md"]}),
    ("hours_estimate", {"branch": DEFAULT}),
    ("hours_estimate", {"branch": DEFAULT, "date_source": "author"}),
    ("hours_estimate", {"branch": DEFAULT, "committer": False, "date_source": "author", "single_commit_hours": 1}),
    ("hours_estimate", {"branch": FEATURE, "grouping_window": 2, "include_globs": ["*.md"]}),
    ("punchcard", {"branch": DEFAULT}),
    ("punchcard", {"branch": DEFAULT, "by": "committer", "normalize": 100}),
    ("punchcard", {"branch": FEATURE, "ignore_globs": ["src/*"]}),
]

COMMON_OWNERSHIP = [
    ("blame", {}),
    ("blame", {"committer": False, "by": "file"}),
    ("blame", {"include_globs": ["*.md"]}),
    ("blame", {"ignore_globs": ["*.md"], "by": "file"}),
    ("bus_factor", {}),
    ("bus_factor", {"by": "file", "ignore_globs": ["src/*"]}),
    ("cumulative_blame", {"branch": DEFAULT}),
    ("cumulative_blame", {"branch": FEATURE, "committer": False}),
    ("cumulative_blame", {"branch": DEFAULT, "num_datapoints": 3, "ignore_globs": ["*.md"]}),
    ("file_detail", {}),
    ("file_detail", {"rev": "v1.0", "committer": False, "include_globs": ["*.md"]}),
    ("revs", {"branch": DEFAULT}),
    ("revs", {"branch": FEATURE, "limit": 2, "skip": 1}),
    ("revs", {"branch": DEFAULT, "num_datapoints": 3}),
    ("tags", {}),
    ("commits_in_tags", {"start": EARLY}),
    ("is_bare", {}),
    ("branches", {}),
    ("has_coverage", {}),
]

REPOSITORY_CASES = [
    *COMMON_ACTIVITY,
    *COMMON_OWNERSHIP,
    ("parallel_cumulative_blame", {"branch": DEFAULT}),
    ("parallel_cumulative_blame", {"branch": FEATURE, "committer": False, "num_datapoints": 3}),
    ("file_owner", {"rev": "HEAD", "filename": "README.md"}),
    ("file_owner", {"rev": "HEAD", "filename": "README.md", "committer": False}),
    ("get_branches_by_commit", {"commit": "v1.0"}),
    ("get_commit_content", {"rev": "v2.0"}),
    ("get_commit_content", {"rev": "v2.0", "ignore_globs": ["*.md"]}),
    ("get_file_content", {"path": "README.md", "rev": "v1.0"}),
    ("get_file_content", {"path": "README.md", "rev": "v2.0"}),
    ("list_files", {"rev": "v1.0"}),
    ("list_files", {}),
    ("has_branch", {"branch": FEATURE}),
    ("has_branch", {"branch": "nope"}),
    ("blame", {"rev": "v1.0", "include_globs": ["*.md"]}),
    ("revs", {"branch": DEFAULT, "skip_broken": True}),
    ("time_between_revs", {"rev1": "v1.0", "rev2": "v2.0"}),
    ("diff_stats_between_revs", {"rev1": "v1.0", "rev2": "v2.0"}),
    ("diff_stats_between_revs", {"rev1": "v1.0", "rev2": "v2.0", "ignore_globs": ["*.md"]}),
    ("committers_between_revs", {"rev1": "v1.0", "rev2": "v2.0", "include_globs": ["*.md"]}),
    ("files_changed_between_revs", {"rev1": "v1.0", "rev2": "v2.0", "ignore_globs": ["src/*"]}),
    ("release_tag_summary", {}),
    ("release_tag_summary", {"tag_glob": "v1*", "ignore_globs": ["*.md"]}),
]

PROJECT_CASES = [
    *COMMON_ACTIVITY,
    *COMMON_OWNERSHIP,
    ("punchcard", {"branch": DEFAULT, "by": "committer"}),
    ("hours_estimate", {"branch": DEFAULT, "by": "committer"}),
    ("cumulative_blame", {"branch": DEFAULT, "by": "repository"}),
    ("blame", {"by": "repository"}),
    ("bus_factor", {"by": "repository"}),
    ("bus_factor", {"by": "projectd", "ignore_globs": ["*.md"]}),
    ("repo_information", {}),
]

EXCLUDED = {
    Repository: {
        "repo_name": "property derived from the working dir; no analysis, not cached",
        "coverage": "needs a .coverage data file and the optional coverage extra",
        "safe_fetch_remote": "network/administration operation, not an analysis result",
        "warm_cache": "cache administration; its effect is covered in test_cache_warming.py",
        "invalidate_cache": "cache administration; covered in test_cache_management.py",
        "get_cache_stats": "cache administration; reports cache contents, not an analysis",
    },
    ProjectDirectory: {
        "repo_name": "property derived from the working dirs; no analysis, not cached",
        "coverage": "needs a .coverage data file and the optional coverage extra",
        "bulk_fetch_and_warm": "network/administration operation, not an analysis result",
        "invalidate_cache": "cache administration; covered in test_cache_management.py",
        "get_cache_stats": "cache administration; reports cache contents, not an analysis",
    },
}


def _resolve(kwargs, default_branch):
    return {k: (default_branch if v is DEFAULT else v) for k, v in kwargs.items()}


def _call(obj, name, kwargs, default_branch):
    attr = getattr(obj, name)
    if not callable(attr):
        return attr
    return attr(**_resolve(kwargs, default_branch))


def _snapshot(value):
    if isinstance(value, pd.DataFrame | pd.Series):
        return value.copy()
    return copy.deepcopy(value)


def _same(left, right):
    if isinstance(left, pd.DataFrame | pd.Series):
        return (
            type(left) is type(right)
            and left.equals(right)
            and list(left.index.names) == list(right.index.names)
            and (not isinstance(left, pd.DataFrame) or list(left.columns) == list(right.columns))
        )
    return left == right


def _case_id(case):
    name, kwargs = case
    shown = ",".join(f"{k}={'<default>' if v is DEFAULT else v!r}" for k, v in kwargs.items())
    return f"{name}({shown})"


def _make_repository(paths, default_branch, cache):
    return Repository(working_dir=str(paths[0]), default_branch=default_branch, cache_backend=cache)


def _make_project(paths, default_branch, cache):
    return ProjectDirectory(
        working_dir=[str(p) for p in paths], default_branch=default_branch, cache_backend=cache, verbose=False
    )


LAYERS = {
    "repository": (_make_repository, REPOSITORY_CASES, Repository),
    "project": (_make_project, PROJECT_CASES, ProjectDirectory),
}


@pytest.fixture(scope="module", params=list(LAYERS))
def layer(request, parity_dirs, default_branch):
    """(cases, uncached instance, cached instance) for one layer; shared across the module."""
    make, cases, _ = LAYERS[request.param]
    return cases, make(parity_dirs, default_branch, None), make(parity_dirs, default_branch, EphemeralCache())


def test_every_public_method_is_listed():
    """A new public method must be added to the table (or EXCLUDED with a reason)."""
    for cls, (_, cases, _) in ((c, LAYERS[n]) for n, c in (("repository", Repository), ("project", ProjectDirectory))):
        public = {n for n in dir(cls) if not n.startswith("_")}
        listed = {name for name, _ in cases} | set(EXCLUDED[cls])
        assert public - listed == set(), (
            f"{cls.__name__} methods missing from the parity table: {sorted(public - listed)}"
        )
        stale = listed - public
        assert stale == set(), f"{cls.__name__} parity table names methods that no longer exist: {sorted(stale)}"
        assert not ({name for name, _ in cases} & set(EXCLUDED[cls]))


def test_cached_equals_uncached_and_cache_hit(layer, default_branch):
    """uncached == cached miss == cached hit, for every table entry."""
    cases, uncached, cached = layer
    for case in cases:
        expected = _call(uncached, case[0], case[1], default_branch)
        miss = _snapshot(_call(cached, case[0], case[1], default_branch))
        hit = _call(cached, case[0], case[1], default_branch)
        assert _same(expected, miss), f"uncached != cached miss for {_case_id(case)}"
        assert _same(miss, hit), f"cached miss != cached hit for {_case_id(case)}"


def test_interleaved_calls_leave_cached_results_unchanged(layer, parity_dirs, default_branch):
    """Run every method in table order, then re-read each one and compare to its snapshot and to uncached."""
    cases, uncached, _ = layer
    make = LAYERS["repository" if cases is REPOSITORY_CASES else "project"][0]
    cached = make(parity_dirs, default_branch, EphemeralCache())

    snapshots = []
    for case in cases:
        snapshots.append((case, _snapshot(_call(cached, case[0], case[1], default_branch))))

    for case, snapshot in snapshots:
        again = _call(cached, case[0], case[1], default_branch)
        assert _same(snapshot, again), f"cached result changed after later calls: {_case_id(case)}"
        assert _same(_call(uncached, case[0], case[1], default_branch), again), (
            f"cached re-read differs from uncached: {_case_id(case)}"
        )


def test_fixture_distinguishes_author_from_committer(parity_dirs, default_branch):
    """Guard the fixture: if these collapse, the date_source / committer entries stop guarding anything."""
    repo = _make_repository(parity_dirs, default_branch, None)
    history = repo.commit_history(branch=default_branch)
    author_history = repo.commit_history(branch=default_branch, date_source="author")
    assert set(history["author"]) != set(history["committer"])
    assert not history.index.equals(author_history.index)
    assert not repo.hours_estimate(branch=default_branch).equals(
        repo.hours_estimate(branch=default_branch, date_source="author")
    )


# Entries whose correct answer on this fixture is legitimately empty/falsy.
EXPECTED_EMPTY = {
    "is_bare",
    "has_coverage",
    "has_branch(branch='nope')",
    "get_commit_content(rev='v2.0',ignore_globs=['*.md'])",
}


def test_table_entries_return_data(layer, default_branch):
    """Equal-but-empty results would pass every parity check vacuously."""
    cases, uncached, _ = layer
    for case in cases:
        value = _call(uncached, case[0], case[1], default_branch)
        empty = value.empty if isinstance(value, pd.DataFrame | pd.Series) else not value
        if _case_id(case) in EXPECTED_EMPTY or case[0] in EXPECTED_EMPTY:
            continue
        assert not empty, f"{_case_id(case)} is empty on the fixture, so it guards nothing"
