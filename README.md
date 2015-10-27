Git-Pandas
==========

A simple set of wrappers around gitpython for creating pandas dataframes out of git data.


Examples / Usage
----------------

A repository is just 1 git repo:
    
    from git import Repo
    import os
    from pandas import DataFrame, to_datetime, set_option
    import datetime
    import sys
    import numpy as np

    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    # build an example repository object and try some things out
    dir = ''
    ignore_dirs = [
        'docs',
        'tests',
        'Data'
    ]
    r = Repository(dir)

    # is it bare?
    print('\nRepo bare?')
    print(r.is_bare())
    print('\n')

    # get the commit history
    ch = r.commit_history('develop', limit=None, extensions=['py'], ignore_dir=ignore_dirs)
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
    fh = r.file_change_history('develop', limit=None, ignore_dir=ignore_dirs)
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

A project is a collection of repos:

    import os
    import sys
    from git import Repo, GitCommandError
    import numpy as np
    from pandas import DataFrame, set_option
    from gitpandas.repository import Repository
    
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    p = ProjectDirectory(working_dir='/foo/bar/')

    # get the commit history
    ch = p.commit_history('develop', limit=None)
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
