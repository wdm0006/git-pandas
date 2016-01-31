from gitpandas.utilities.plotting import plot_cumulative_blame
from gitpandas import GitHubProfile

__author__ = 'willmcginnis'


if __name__ == '__main__':
    g = GitHubProfile(username='wdm0006', ignore_forks=True, verbose=True)
    blame = g.cumulative_blame(branch='master', extensions=['py'], by='project', ignore_dir=['docs'], limit=None, skip=None)
    plot_cumulative_blame(blame)