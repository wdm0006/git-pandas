import os
from gitpandas import Repository

from definitions import GIT_PANDAS_DIR

__author__ = 'willmcginnis'


if __name__ == '__main__':
    repo = Repository(working_dir=GIT_PANDAS_DIR)
    fc = repo.file_change_rates(include_globs=['*.py'], coverage=True)
    print(fc)