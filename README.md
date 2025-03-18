Git-Pandas
==========

![license](https://img.shields.io/pypi/l/Django.svg) [![PyPI version](https://badge.fury.io/py/git-pandas.svg)](https://badge.fury.io/py/git-pandas) ![downloads](https://img.shields.io/pypi/dm/git-pandas.svg) 

Git-Pandas is a powerful Python library that transforms Git repository data into pandas DataFrames, making it easy to analyze and visualize your codebase's history, contributors, and development patterns. Built on top of GitPython, it provides a simple yet powerful interface for extracting meaningful insights from your Git repositories.

![Cumulative Blame](https://raw.githubusercontent.com/wdm0006/git-pandas/master/examples/img/githubblame.png)

## Why Git-Pandas?

- **Easy to Use**: Simple API that converts Git data into familiar pandas DataFrames
- **Comprehensive Analysis**: From basic commit history to complex metrics like bus factor
- **Flexible**: Works with single repositories or entire project directories
- **Visualization Ready**: Built-in plotting utilities for common Git analytics
- **Performance Optimized**: Optional caching support for memory-intensive operations

## Core Components

### Repository
The `Repository` class provides a wrapper around a single Git repository, offering methods to:
- Extract commit history with filtering by extension and directory
- Analyze file changes and blame information
- Track branch and tag information
- Generate cumulative blame statistics
- Calculate file ownership and contribution patterns

### ProjectDirectory
The `ProjectDirectory` class enables analysis across multiple repositories:
- Automatically discovers and analyzes nested Git repositories
- Aggregates metrics across multiple repositories
- Provides project-level insights and statistics
- Calculates cross-repository metrics like total development time

## Key Features

### Repository Analysis
- **Commit History**: Track changes with extension and directory filtering
- **File Analysis**: Monitor edited files and blame information
- **Branch & Tag Management**: Access repository structure information
- **Cumulative Blame**: Generate time-series data of code ownership
- **File Ownership**: Approximate file ownership and contribution patterns

### Project Insights
- **Bus Factor**: Calculate project sustainability metrics
- **Development Time**: Estimate hours spent per project or author
- **Contributor Analysis**: Track individual and team contributions
- **Project Health**: Generate comprehensive project information tables

### GitHub Integration
- **Profile Analysis**: Analyze GitHub.com profiles via `GitHubProfile` object
- **Repository Metrics**: Extract repository-specific insights
- **Contributor Insights**: Track external contributions and collaborations

### Visualization Tools
- **Plotting Helpers**: Built-in utilities for common Git analytics
- **Punchcard Analysis**: Generate and visualize commit patterns
- **Blame Visualization**: Create cumulative blame charts
- **Time Series Analysis**: Track changes and patterns over time

## Installation

Git-Pandas supports Python 2.7+ and 3.3+. Install using pip:

```bash
pip install git-pandas
```

## Quick Start

```python
from gitpandas import Repository

# Analyze a single repository
repo = Repository('/path/to/repo')
commits_df = repo.commit_history()
blame_df = repo.blame()

# Analyze multiple repositories
from gitpandas import ProjectDirectory
project = ProjectDirectory('/path/to/project')
project_info = project.general_information()
```

## Documentation

Comprehensive documentation is available at [http://wdm0006.github.io/git-pandas/](http://wdm0006.github.io/git-pandas/)

## Performance Optimization

For memory-intensive operations, Git-Pandas supports:
- Memory-based caching
- Redis-based caching
- Configurable cache durations

## Projects Using Git-Pandas

- [GitNOC](https://github.com/wdm0006/gitnoc): Network of Code analysis tool
- [Commit Opener](https://github.com/lbillingham/commit_opener): Commit analysis and visualization tool

## Contributing

We welcome contributions! Please review our [Contributing Guidelines](CONTRIBUTING.md) for details on:
- Code of Conduct
- Development Setup
- Pull Request Process
- Starter Issues

## License

This project is BSD licensed (see [LICENSE.md](LICENSE.md))