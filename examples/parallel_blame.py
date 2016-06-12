from gitpandas import Repository
import time

__author__ = 'willmcginnis'


if __name__ == '__main__':
    g = Repository(working_dir='..')

    st = time.time()
    blame = g.cumulative_blame(branch='master', extensions=['py', 'html', 'sql', 'md'], limit=None, skip=None)
    print(blame.head())
    print(time.time() - st)

    st = time.time()
    blame = g.parallel_cumulative_blame(branch='master', extensions=['py', 'html', 'sql', 'md'], limit=None, skip=None, workers=4)
    print(blame.head())
    print(time.time() - st)