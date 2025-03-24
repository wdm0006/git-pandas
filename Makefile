.PHONY: setup test test-all lint format clean docs build run-example test-single

# Use uv for all Python operations
PYTHON = python
UV = uv

# Project settings
PACKAGE_NAME = gitpandas
TESTS_DIR = tests
DOCS_DIR = docs
BUILD_DIR = dist
EXAMPLES_DIR = examples

setup:
	$(UV) pip install -e ".[dev]"

setup-examples:
	$(UV) pip install -e ".[examples]"

setup-all:
	$(UV) pip install -e ".[all]"

test:
	MPLBACKEND=Agg $(UV) run pytest $(TESTS_DIR) --cov=$(PACKAGE_NAME) --cov-report=term-missing -m "not slow"

test-single:
	@if [ "$(test)" = "" ]; then \
		echo "Error: Please specify a test using test=<path_to_test>"; \
		echo "Example: make test-single test=tests/test_Repository/test_advanced.py::TestRepositoryAdvanced::test_parallel_cumulative_blame"; \
		exit 1; \
	fi
	MPLBACKEND=Agg $(UV) run pytest $(test) -v

test-all:
	MPLBACKEND=Agg $(UV) run pytest $(TESTS_DIR) --cov=$(PACKAGE_NAME) --cov-report=term-missing

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix --unsafe-fixes .

docs:
	$(MAKE) -C $(DOCS_DIR) html

docs-serve:
	cd $(DOCS_DIR)/build/html && $(PYTHON) -m http.server

clean:
	rm -rf $(BUILD_DIR)
	rm -rf $(DOCS_DIR)/build
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build:
	$(UV) pip build

publish:
	$(UV) pip publish

env-export:
	$(UV) pip freeze > requirements.txt

run-example:
	@if [ "$(example)" = "" ]; then \
		echo "Error: Please specify an example to run using example=<name>"; \
		echo "Available examples:"; \
		ls $(EXAMPLES_DIR)/*.py | sed 's/$(EXAMPLES_DIR)\///' | sed 's/\.py$$//'; \
		exit 1; \
	fi
	@if [ ! -f "$(EXAMPLES_DIR)/$(example).py" ]; then \
		echo "Error: Example '$(example)' not found in $(EXAMPLES_DIR)"; \
		exit 1; \
	fi
	MPLBACKEND=Agg $(UV) run python $(EXAMPLES_DIR)/$(example).py

help:
	@echo "Available commands:"
	@echo "  setup         Install the package in development mode"
	@echo "  setup-examples Install the package with examples dependencies"
	@echo "  setup-all     Install the package with all dependencies"
	@echo "  test          Run tests with pytest (excluding slow tests)"
	@echo "  test-single   Run a single test (usage: make test-single test=<path_to_test>)"
	@echo "  test-all      Run all tests including slow tests"
	@echo "  lint          Run ruff linter"
	@echo "  format        Format code with ruff"
	@echo "  docs          Build documentation with Sphinx"
	@echo "  docs-serve    Serve documentation locally"
	@echo "  clean         Remove build artifacts"
	@echo "  build         Build distribution packages"
	@echo "  publish       Publish package to PyPI"
	@echo "  env-export    Export dependencies to requirements.txt"
	@echo "  run-example   Run a specific example (usage: make run-example example=<name>)" 