.PHONY: setup run clean test

# Default target
all: run

# Requires 'uv' to be installed: https://github.com/astral-sh/uv
# Check if virtual environment exists, create if not using uv
VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python

# Setup: Install dependencies into a virtual environment using uv
setup:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "Creating virtual environment in $(VENV_DIR) using uv..."; \
		uv venv $(VENV_DIR); \
	else \
		echo "Virtual environment $(VENV_DIR) already exists."; \
	fi
	@echo "Installing dependencies from requirements.txt using uv..."
	@uv pip install -r requirements.txt --python $(PYTHON)
	@echo "Setup complete."

# Run: Install dependencies (if needed) and run the application
run: setup
	@echo "Running GitNOC Desktop application..."
	@$(PYTHON) main.py

# Clean: Remove virtual environment and __pycache__
clean:
	@echo "Cleaning up..."
	@rm -rf $(VENV_DIR)
	@find . -type d -name "__pycache__" -exec rm -r {} +
	@echo "Cleanup complete."

# Test: Run tests using pytest through uv
# Accepts PYTEST_ARGS for passing extra arguments to pytest
# Example: make test PYTEST_ARGS="-k my_test_name"
test: setup
	@echo "Running tests..."
	@echo "Using Python: $(PYTHON)"
	@$(PYTHON) -m pytest $(PYTEST_ARGS) 