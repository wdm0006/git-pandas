.PHONY: setup test lint format clean docs build

# Use uv for all Python operations
PYTHON = python
UV = uv

# Project settings
PACKAGE_NAME = gitpandas
TESTS_DIR = tests
DOCS_DIR = docs
BUILD_DIR = dist

setup:
	$(UV) pip install -e ".[dev]"

setup-examples:
	$(UV) pip install -e ".[examples]"

setup-all:
	$(UV) pip install -e ".[all]"

test:
	MPLBACKEND=Agg $(UV) run pytest $(TESTS_DIR) --cov=$(PACKAGE_NAME) --cov-report=term-missing

lint:
	$(UV) run ruff check .

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

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

help:
	@echo "Available commands:"
	@echo "  setup         Install the package in development mode"
	@echo "  setup-examples Install the package with examples dependencies"
	@echo "  setup-all     Install the package with all dependencies"
	@echo "  test          Run tests with pytest"
	@echo "  lint          Run ruff linter"
	@echo "  format        Format code with ruff"
	@echo "  docs          Build documentation with Sphinx"
	@echo "  docs-serve    Serve documentation locally"
	@echo "  clean         Remove build artifacts"
	@echo "  build         Build distribution packages"
	@echo "  publish       Publish package to PyPI"
	@echo "  env-export    Export dependencies to requirements.txt" 