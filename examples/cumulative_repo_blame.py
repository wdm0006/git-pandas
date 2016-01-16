import os
from gitpandas.utilities.plotting import plot_cumulative_blame
from gitpandas import Repository

__author__ = 'willmcginnis'


if __name__ == '__main__':
    g = Repository(working_dir=os.path.abspath('../../git-pandas'), verbose=True)
    blame = g.cumulative_blame(branch='master', extensions=['py'], ignore_dir=['docs'], limit=None, skip=None)
    plot_cumulative_blame(blame)