import os
from gitpandas import ProjectDirectory

from definitions import GIT_PANDAS_DIR

__author__ = 'willmcginnis'

if __name__ == '__main__':
    g = ProjectDirectory(working_dir=GIT_PANDAS_DIR)

    b = g.blame(include_globs=['*.py'], ignore_globs=['lib/*', 'docs/*'], by='file')
    print(b.head(5))

