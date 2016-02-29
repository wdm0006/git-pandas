import os
from gitpandas import Repository, ProjectDirectory

__author__ = 'willmcginnis'

if __name__ == '__main__':
    # g = Repository(working_dir=os.path.abspath('../'))
    g = ProjectDirectory(working_dir=os.path.abspath('../'))

    b = g.file_detail(extensions=['py'], ignore_dir=['lib', 'docs'])
    print(b.head(25))

