from gitpandas.utilities import __version__


def test_version():
    """Test that the version is defined."""
    assert isinstance(__version__, str)
    assert len(__version__.split(".")) >= 2  # Should have at least major.minor version
