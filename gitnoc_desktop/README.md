# GitNOC Desktop

A basic native desktop application for Git repository analytics using PySide6 and git-pandas.

## Setup

1.  **Create a virtual environment (optional but recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

```bash
python main.py
```

## Features

*   Add local Git repositories using a native directory browser.
*   View a list of added repositories.
*   Select a repository to view basic information (path, latest commit).
*   Repositories are saved between sessions. 