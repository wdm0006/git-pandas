import os
from gitpandas import ProjectDirectory, Repository
import numpy as np
from pandas import set_option

__author__ = 'willmcginnis'


def project(path):
    p = ProjectDirectory(working_dir=path)

    # get the commit history
    ch = p.commit_history('master', limit=None, extensions=['py'], ignore_dir=['lib', 'docs', 'test', 'tests', 'tests_t'], days=7)
    print(ch.head)

    # get the list of committers
    print('\nCommiters:')
    print(''.join([str(x) + '\n' for x in set(ch['committer'].values)]))
    print('\n')

    # print out everyone's contributions
    attr = ch.reindex(columns=['committer', 'lines', 'insertions', 'deletions', 'net']).groupby(['committer'])
    attr = attr.agg({
        'lines': np.sum,
        'insertions': np.sum,
        'deletions': np.sum,
        'net': np.sum
    })
    print(attr)


def repository(path):
    # build an example repository object and try some things out
    ignore_dirs = [
        'docs',
        'tests',
        'Data'
    ]
    r = Repository(path)

    # is it bare?
    print('\nRepo bare?')
    print(r.is_bare())
    print('\n')

    # get the commit history
    ch = r.commit_history('HEAD', limit=None, extensions=['py'], ignore_dir=ignore_dirs)
    print(ch.head(5))

    # get the list of committers
    print('\nCommiters:')
    print(''.join([str(x) + '\n' for x in set(ch['committer'].values)]))
    print('\n')

    # print out everyone's contributions
    attr = ch.reindex(columns=['committer', 'lines', 'insertions', 'deletions']).groupby(['committer'])
    attr = attr.agg({
        'lines': np.sum,
        'insertions': np.sum,
        'deletions': np.sum
    })
    print(attr)

    # get the file change history
    fh = r.file_change_history('HEAD', limit=None, ignore_dir=ignore_dirs)
    fh['ext'] = fh['filename'].map(lambda x: x.split('.')[-1])
    print(fh.head(50))

    # print out unique extensions
    print('\nExtensions Found:')
    print(''.join([str(x) + '\n' for x in set(fh['ext'].values)]))
    print('\n')

    # agg by extension
    etns = fh.reindex(columns=['ext', 'insertions', 'deletions']).groupby(['ext'])
    etns = etns.agg({
        'insertions': np.sum,
        'deletions': np.sum
    })
    print(etns)

if __name__ == '__main__':
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    path = os.path.abspath('../../git-pandas')
    project(path)
    repository(path)