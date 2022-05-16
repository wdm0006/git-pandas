import os
from gitpandas.repository import Repository

from definitions import GIT_PANDAS_DIR

__author__ = 'willmcginnis'


# build an example repository object and try some things out
ignore_dirs = ['tests/*']
r = Repository(GIT_PANDAS_DIR, verbose=True)

# get the hours estimate for this repository (using 30 mins per commit)
he = r.hours_estimate(
    branch='master',
    grouping_window=0.5,
    single_commit_hours=0.75,
    limit=None,
    include_globs=['*.py'],
    ignore_globs=ignore_dirs
)
print(he)
