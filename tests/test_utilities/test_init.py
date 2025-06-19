import gitpandas


def test_version():
    """Test that the version is defined."""
    assert isinstance(gitpandas.__version__, str)
    assert len(gitpandas.__version__.split(".")) >= 2  # Should have at least major.minor version
