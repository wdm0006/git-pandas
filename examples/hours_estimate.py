import os
from gitpandas.repository import Repository

__author__ = 'willmcginnis'

# get the path of this repo
path = os.path.abspath('../../git-pandas')

# build an example repository object and try some things out
ignore_dirs = ['tests']
r = Repository(path, verbose=True)

# get the hours estimate for this repository (using 30 mins per commit)
he = r.hours_estimate(
    branch='master',
    grouping_window=0.5,
    single_commit_hours=0.75,
    limit=None,
    extensions=['py'],
    ignore_dir=ignore_dirs
)
print(he)
