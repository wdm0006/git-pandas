Contributing to Git-Pandas
==========================

We welcome contributions to Git-Pandas! Our goal is to make Git repository analysis simple, fast, and accessible to developers interested in data analysis.

Getting Started
---------------

1. Fork the repository on GitHub
2. Clone your fork locally:

.. code-block:: shell

    git clone git@github.com:YourLogin/git-pandas.git
    cd git-pandas

3. Create a feature branch:

.. code-block:: shell

    git checkout -b feature/your-awesome-feature

4. Make your changes
5. Submit a pull request

Development Guidelines
----------------------

Code Style
~~~~~~~~~~

* Follow PEP 8 style guide
* Use 4 spaces for indentation
* Maximum line length of 88 characters (Black default)
* Use snake_case for variables and functions
* Use CamelCase for classes
* Add docstrings following Google style guide

Documentation
~~~~~~~~~~~~~

* Write detailed docstrings for all public APIs
* Include type hints for function parameters and return values
* Document exceptions that may be raised
* Add examples where appropriate
* Update the documentation for any new features

Testing
~~~~~~~

* Write unit tests for new code
* Maintain or improve test coverage
* Run tests before submitting PR:

.. code-block:: shell

    make test
    make coverage

API Design Principles
~~~~~~~~~~~~~~~~~~~~~

* Maintain feature parity between Repository and ProjectDirectory
* Include limit options for memory-intensive functions
* Keep the API simple and intuitive
* Consider performance implications

Current Development Focus
-------------------------

High Priority
~~~~~~~~~~~~~

* Improve test coverage with proper unit tests
* Add diff functionality between revisions
* Enhance documentation with more examples and visualizations
* Streamline documentation deployment

Feature Ideas
~~~~~~~~~~~~~

* File-level history tracking
* Cross-branch analytics
* Enhanced verbose logging
* Hierarchical bus factor analysis
* Language analytics and insights

Development Setup
-----------------

1. Install development dependencies:

.. code-block:: shell

    make install-dev

2. Set up pre-commit hooks:

.. code-block:: shell

    make pre-commit

3. Run tests:

.. code-block:: shell

    make test

4. Build documentation:

.. code-block:: shell

    make docs

Questions?
----------

* Open an issue for bug reports or feature requests
* Join our discussions on GitHub
* Check existing issues for similar problems

Thank you for contributing to Git-Pandas!

