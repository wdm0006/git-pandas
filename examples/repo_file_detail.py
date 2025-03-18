from definitions import GIT_PANDAS_DIR

from gitpandas import ProjectDirectory

__author__ = "willmcginnis"

if __name__ == "__main__":
    g = ProjectDirectory(working_dir=GIT_PANDAS_DIR)

    b = g.file_detail(include_globs=["*.py"], ignore_globs=["lib/*", "docs/*"])
    print(b.head(25))
