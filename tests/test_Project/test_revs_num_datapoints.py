"""Regression tests for ``ProjectDirectory.revs(num_datapoints=...)`` sample sizes.

The project layer divides ``num_datapoints`` across its member repositories and then
delegates to ``Repository.revs``, so it inherited the over-sampling fixed for the single
repository case.
"""

import pytest

from gitpandas import ProjectDirectory
from gitpandas.cache import EphemeralCache
from tests.test_Repository.test_revs_num_datapoints import COMMIT_COUNT, build_revs_repo

REPO_COUNT = 2


@pytest.fixture
def project_dirs(tmp_path, default_branch):
    return [build_revs_repo(tmp_path / f"revs_repo_{index}", default_branch) for index in range(REPO_COUNT)]


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
def project(request, project_dirs, default_branch):
    cache_backend = EphemeralCache() if request.param else None
    projectd = ProjectDirectory(
        working_dir=[str(path) for path in project_dirs],
        default_branch=default_branch,
        cache_backend=cache_backend,
        verbose=False,
    )
    yield projectd
    projectd.__del__()


class TestProjectRevsNumDatapoints:
    @pytest.mark.parametrize("num_datapoints", [2, 4, 6, 10])
    def test_returns_the_requested_sample_size_across_repos(self, project, num_datapoints):
        df = project.revs(num_datapoints=num_datapoints)

        assert df.shape[0] == num_datapoints
        # evenly divided between the member repositories
        assert df["repository"].value_counts().tolist() == [num_datapoints // REPO_COUNT] * REPO_COUNT

    def test_request_beyond_history_returns_each_rev_once(self, project):
        df = project.revs(num_datapoints=COMMIT_COUNT * REPO_COUNT * 3)

        assert df.shape[0] == COMMIT_COUNT * REPO_COUNT
        assert not df.duplicated(subset=["repository", "rev"]).any()

    def test_request_smaller_than_the_repo_count_keeps_one_rev_per_repo(self, project):
        """A positive request must never round down to zero datapoints per repository."""
        df = project.revs(num_datapoints=1)

        assert df.shape[0] == REPO_COUNT
        assert sorted(df["repository"].tolist()) == sorted(repo.repo_name for repo in project.repos)

    @pytest.mark.parametrize("num_datapoints", [0, -1])
    def test_non_positive_num_datapoints_raises(self, project, num_datapoints):
        with pytest.raises(ValueError, match="num_datapoints must be a positive integer"):
            project.revs(num_datapoints=num_datapoints)
