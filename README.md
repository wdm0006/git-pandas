Git-Pandas
==========

v0.0.2

A simple set of wrappers around gitpython for creating pandas dataframes out of git data. The project is centered around
two primary objects:

 * Repository()
 * ProjectDirectory()
 
A Repository object contains a single git repo, and is used to interact with it.  A ProjectDirectory references a directory
in your filesystem which may have in it multiple git repositories. The subdirectories are all walked to find any child
repos, and any analysis is aggregated up from all of those into a single output (pandas dataframe).

Current functionality includes:

 * Commit history with extension and directory filtering
 * Edited files history with extension and directory filtering
 * Blame with extension and directory filtering
 * Branches 
 * Tags
 * ProjectDirectory-level general information table
  
Please see examples for more detailed usage.

Installation
------------

To install use:

    pip install git-pandas
    
Documentation
-------------

Docs can be found here: [http://wdm0006.github.io/git-pandas/](http://wdm0006.github.io/git-pandas/)

Examples / Usage
----------------

A repository is just 1 git repo:
    
    from gitpandas import Repository
    from pandas import set_option

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

A project is a collection of repositories:

    from pandas import set_option
    from gitpandas import ProjectDirectory
    
    set_option('display.height', 1000)
    set_option('display.max_rows', 500)
    set_option('display.max_columns', 500)
    set_option('display.width', 1000)

    p = ProjectDirectory(working_dir='/foo/bar/')

    # get the commit history
    ch = p.commit_history('develop', limit=None)
    print(ch.head(5))

    # get the list of committers
    print('\nCommitters:')
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

Contributing
------------

If you'd like to contribute, let me know, or just submit a pull request. We have no specific long term goals or guidelines
at this stage.

License
-------

This is BSD licensed (see License.md)