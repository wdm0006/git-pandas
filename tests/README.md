# Git-Pandas Tests

This directory contains the test suite for the git-pandas library.

## Running Tests

You can run the tests using the Makefile:

```bash
# Run all tests
make test

# Run tests with coverage report
make test-cov
```

Or directly with pytest:

```bash
# Run all tests
python -m pytest

# Run tests without remote tests
python -m pytest -m "not remote"

# Run specific test file
python -m pytest tests/test_Repository/test_properties.py

# Run specific test
python -m pytest tests/test_Repository/test_properties.py::TestLocalProperties::test_repo_name
```

## Test Structure

- `test_Repository/`: Tests for the Repository class
- `test_Project/`: Tests for the ProjectDirectory class

## Test Markers

- `remote`: Tests that require internet connection (can be skipped with `-m "not remote"`) 