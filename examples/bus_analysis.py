"""
Assumes that GitPython and pandas are in the same directory as this repo, and nothing else is in that directory.
"""

from gitpandas import Repository

__author__ = 'willmcginnis'


if __name__ == '__main__':
    flask_repo = Repository(working_dir='git://github.com/mitsuhiko/flask.git')

    # do some blaming
    flask_blame = flask_repo.blame(extensions=['py'])

    # figure out committer count from each
    flask_ch = flask_repo.commit_history('master', limit=None, extensions=['py'])

    print('\tflask committers: %d' % (len(set(flask_ch['committer'].values))))
    print('\tflask bus count:')
    print(flask_repo.bus_factor(extensions=['py']))