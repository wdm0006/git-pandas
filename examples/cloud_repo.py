from gitpandas import Repository

__author__ = 'willmcginnis'


if __name__ == '__main__':
    repo = Repository(working_dir='git://github.com/CamDavidsonPilon/lifelines.git', verbose=True)
    shared_blame = repo.blame(extensions=['py'], committer=False, by='file')

    print(shared_blame)
