import inspect

import pytest

from gitpandas.utilities.check_api import (
    extract_objects,
    get_distinct_params,
    parse_docstring,
)


class TestCheckAPI:
    def test_extract_objects(self):
        """Test extracting objects from a module."""
        # Test with classes only
        objects = extract_objects(pytest, classes=True, functions=False)
        assert isinstance(objects, dict)
        assert all(inspect.isclass(v) for v in objects.values())

        # Test with functions only
        objects = extract_objects(pytest, classes=False, functions=True)
        assert isinstance(objects, dict)
        assert all(inspect.isfunction(v) for v in objects.values())

        # Test with both
        objects = extract_objects(pytest, classes=True, functions=True)
        assert isinstance(objects, dict)
        assert any(inspect.isclass(v) for v in objects.values())
        assert any(inspect.isfunction(v) for v in objects.values())

    def test_parse_docstring(self):
        """Test parsing docstrings to extract parameter information."""
        # Test with a simple docstring
        doc = """
        Test function.
        
        :param arg1: First argument
        :param arg2: Second argument
        """
        params = parse_docstring(doc)
        assert len(params) == 2
        assert params[0]["arg1"] == "First argument"
        assert params[1]["arg2"] == "Second argument"

        # Test with no parameters
        doc = "Test function with no parameters."
        params = parse_docstring(doc)
        assert len(params) == 0

    def test_get_distinct_params(self):
        """Test getting distinct parameters from signatures."""
        # Create test signatures
        sigs = {
            "func1": {"args": ["arg1", "arg2"]},
            "func2": {"args": ["arg2", "arg3"]},
            "func3": {"args": ["arg1", "arg3"]},
        }

        # Get distinct parameters
        params = get_distinct_params(sigs)

        # Should have all unique parameters
        assert params == {"arg1", "arg2", "arg3"}
