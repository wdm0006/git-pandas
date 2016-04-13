from gitpandas.utilities.plotting import plot_cumulative_blame
from gitpandas import GitHubProfile, ProjectDirectory

__author__ = 'willmcginnis'


if __name__ == '__main__':
    #g = GitHubProfile(username='wdm0006', ignore_forks=True, verbose=True)
    g = ProjectDirectory(working_dir=['git://github.com/rhiever/tpot.git'])
    blame = g.cumulative_blame(branch='master', extensions=['py', 'html', 'sql', 'md'], by='committer', limit=None, skip=None)
    plot_cumulative_blame(blame)