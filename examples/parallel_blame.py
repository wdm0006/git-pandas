from gitpandas import Repository
import time

from definitions import GIT_PANDAS_DIR

__author__ = 'willmcginnis'


if __name__ == '__main__':
    g = Repository(working_dir=GIT_PANDAS_DIR)

    st = time.time()
    blame = g.cumulative_blame(branch='master', include_globs=['*.py', '*.html', '*.sql', '*.md'], limit=None, skip=None)
    print(blame.head())
    print(time.time() - st)

    st = time.time()
    blame = g.parallel_cumulative_blame(branch='master', include_globs=['*.py', '*.html', '*.sql', '*.md'], limit=None, skip=None, workers=4)
    print(blame.head())
    print(time.time() - st)