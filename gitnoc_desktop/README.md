# GitNOC Desktop

Desktop application for Git repository analytics using PySide6 and git-pandas.

## Setup

1. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Running

```bash
python main.py
```

## Features

* Add local Git repositories using a native directory browser
* View repository data through specialized tabs:
  * Overview - general repository information and statistics
  * Code Health - file details, change rates, and code coverage
  * Contributors - contributor activity and effort analysis
  * Tags - repository tag history
  * Cumulative Blame - authorship changes over time
* Repositories are saved between sessions 