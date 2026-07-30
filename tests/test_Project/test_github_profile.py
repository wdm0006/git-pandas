from unittest.mock import Mock, call, patch

import requests

from gitpandas import GitHubProfile


def _response(repositories, next_url=None):
    response = Mock()
    response.json.return_value = repositories
    response.links = {"next": {"url": next_url}} if next_url else {}
    return response


def _repo(name, fork=False):
    return {"git_url": f"git://github.com/example/{name}.git", "fork": fork}


@patch("gitpandas.project.ProjectDirectory.__init__", autospec=True)
@patch("gitpandas.project.requests.get")
def test_discovers_repositories_from_all_pages_in_api_order(mock_get, mock_project_init):
    next_url = "https://api.github.com/users/example/repos?per_page=100&page=2"
    mock_get.side_effect = [
        _response([_repo("first"), _repo("second")], next_url),
        _response([_repo("third")]),
    ]

    profile = GitHubProfile("example")
    profile.repos = []

    assert mock_get.call_args_list == [
        call("https://api.github.com/users/example/repos", params={"per_page": 100}),
        call(next_url, params=None),
    ]
    mock_project_init.assert_called_once_with(
        profile,
        working_dir=[
            "git://github.com/example/first.git",
            "git://github.com/example/second.git",
            "git://github.com/example/third.git",
        ],
        ignore_repos=None,
        verbose=False,
        default_branch=None,
    )


@patch("gitpandas.project.ProjectDirectory.__init__", autospec=True)
@patch("gitpandas.project.requests.get")
def test_ignore_forks_filters_every_page(mock_get, mock_project_init):
    next_url = "https://api.github.com/users/example/repos?page=2"
    mock_get.side_effect = [
        _response([_repo("first"), _repo("fork-one", fork=True)], next_url),
        _response([_repo("fork-two", fork=True), _repo("second")]),
    ]

    profile = GitHubProfile("example", ignore_forks=True)
    profile.repos = []

    mock_project_init.assert_called_once_with(
        profile,
        working_dir=[
            "git://github.com/example/first.git",
            "git://github.com/example/second.git",
        ],
        ignore_repos=None,
        verbose=False,
        default_branch=None,
    )


@patch("gitpandas.project.ProjectDirectory.__init__", autospec=True)
@patch("gitpandas.project.requests.get")
def test_stops_when_response_has_no_next_link(mock_get, mock_project_init):
    mock_get.return_value = _response([_repo("only")])

    profile = GitHubProfile("example")
    profile.repos = []

    mock_get.assert_called_once_with(
        "https://api.github.com/users/example/repos",
        params={"per_page": 100},
    )


@patch("gitpandas.project.ProjectDirectory.__init__", autospec=True)
@patch("gitpandas.project.requests.get")
def test_later_page_failure_discards_partial_results(mock_get, mock_project_init):
    next_url = "https://api.github.com/users/example/repos?page=2"
    mock_get.side_effect = [
        _response([_repo("partial")], next_url),
        requests.exceptions.ConnectionError("request failed"),
    ]

    profile = GitHubProfile("example")
    profile.repos = []

    mock_project_init.assert_called_once_with(
        profile,
        working_dir=[],
        ignore_repos=None,
        verbose=False,
        default_branch=None,
    )


@patch("gitpandas.project.ProjectDirectory.__init__", autospec=True)
@patch("gitpandas.project.requests.get")
def test_forwards_explicit_default_branch(mock_get, mock_project_init):
    mock_get.return_value = _response([_repo("only")])

    profile = GitHubProfile("example", default_branch="master")
    profile.repos = []

    mock_project_init.assert_called_once_with(
        profile,
        working_dir=["git://github.com/example/only.git"],
        ignore_repos=None,
        verbose=False,
        default_branch="master",
    )
