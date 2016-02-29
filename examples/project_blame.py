import os
from gitpandas import ProjectDirectory

__author__ = 'willmcginnis'

if __name__ == '__main__':
    g = ProjectDirectory(working_dir=os.path.abspath('../'))

    b = g.blame(extensions=['py'], ignore_dir=['lib', 'docs'], by='file')
    print(b.head(5))

