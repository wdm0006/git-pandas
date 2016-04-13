import os
import sys
from gitpandas import *
import numpy as np
from pandas import DataFrame, set_option
from gitpandas.repository import Repository

__author__ = 'willmcginnis'


def repository(path):
    # build an example repository object and try some things out
    ignore_dirs = [
        'lib'
    ]
    r = Repository(path)

    # get the commit history
    ch = r.hours_estimate('HEAD', limit=None, extensions=['py'], ignore_dir=ignore_dirs)
    print(ch)


def project(path):
    # build an example repository object and try some things out
    ignore_dirs = [
        'lib'
    ]
    r = ProjectDirectory(path)

    # get the commit history
    ch = r.hours_estimate('HEAD', limit=None, extensions=['py'], ignore_dir=ignore_dirs, by='project')
    print(ch)

if __name__ == '__main__':
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    path = [os.path.abspath('../../git-pandas')]
    project(path)