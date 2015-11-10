from pandas import set_option
from gitpandas.repository import Repository
from gitpandas.project import ProjectDirectory

__author__ = 'willmcginnis'


def repository(path):
    # build an example repository object then check the branches and tags
    r = Repository(path)

    print('Repository Branches:')
    print(r.branches())
    print('\nRepository Tags:')
    print(r.tags())


def project(path):
    # build an example project directory object then check the branches and tags
    p = ProjectDirectory(path)

    print('Project Branches:')
    print(p.branches())
    print('\nProject Tags:')
    print(p.tags())


if __name__ == '__main__':
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    path = '~/git-pandas'
    repository(path)

    path = '~'
    project(path)
