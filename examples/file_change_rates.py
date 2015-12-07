import os
from gitpandas import Repository

__author__ = 'willmcginnis'


if __name__ == '__main__':
    repo = Repository(working_dir=os.path.abspath('../../git-pandas'))
    fc = repo.file_change_rates(extensions=['py'], coverage=True)
    print(fc)