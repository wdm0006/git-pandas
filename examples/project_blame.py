from gitpandas import ProjectDirectory

__author__ = 'willmcginnis'

if __name__ == '__main__':
    g = ProjectDirectory(working_dir='')

    b = g.blame(extensions=['py'], ignore_dir=['lib', 'docs'])
    info = g.repo_information()
    print(info)
    print(b)
