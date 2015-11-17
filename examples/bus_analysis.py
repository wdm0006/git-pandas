"""
Assumes that GitPython and pandas are in the same directory as this repo, and nothing else is in that directory.
"""

import os
from pandas import merge
from gitpandas import ProjectDirectory, Repository

__author__ = 'willmcginnis'


def get_interfaces():
    project_path = str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    proj = ProjectDirectory(working_dir=project_path)

    pandas_repo = Repository(working_dir=project_path + os.sep + 'pandas')
    gitpython_repo = Repository(working_dir=project_path + os.sep + 'GitPython')

    return proj, pandas_repo, gitpython_repo
if __name__ == '__main__':
    project, pandas_repo, gitpython_repo = get_interfaces()

    # do some blaming
    shared_blame = project.blame(extensions=['py'])
    pandas_blame = pandas_repo.blame(extensions=['py'])
    gitpython_blame = gitpython_repo.blame(extensions=['py'])

    # figure out who is common between projects
    common = merge(pandas_blame, gitpython_blame, how='inner', left_index=True, right_index=True)
    common = common.rename(columns={'loc_x': 'pandas_loc', 'loc_y': 'gitpython_loc'})

    # figure out committer count from each
    pandas_ch = pandas_repo.commit_history('master', limit=None, extensions=['py'])
    gitpython_ch = gitpython_repo.commit_history('master', limit=None, extensions=['py'])

    # now print out some things
    print('Total Python LOC for 3 Projects Combined')
    print('\t%d' % (int(shared_blame['loc'].sum()), ))

    print('\nNumber of contributors per project')
    print('\tPandas: %d' % (len(set(pandas_ch['committer'].values))))
    print('\tGitPython: %d' % (len(set(gitpython_ch['committer'].values))))

    print('\nTop 10 Contributors Between Each')
    print(shared_blame.head(10))

    print('\nCommitters that committed to Both')
    print(common)

    print('\nTruck Count of Each')
    print('\tPandas: %d' % (pandas_repo.bus_factor(extensions=['py'])))
    print('\tGitPython: %d' % (gitpython_repo.bus_factor(extensions=['py'])))