Git-Pandas
==========

![license](https://img.shields.io/pypi/l/Django.svg) [![Coverage Status](https://coveralls.io/repos/wdm0006/git-pandas/badge.svg?branch=master&service=github)](https://coveralls.io/github/wdm0006/git-pandas?branch=master)  ![travis status](https://travis-ci.org/wdm0006/git-pandas.svg?branch=master) [![PyPI version](https://badge.fury.io/py/git-pandas.svg)](https://badge.fury.io/py/git-pandas) ![downloads](https://img.shields.io/pypi/dm/git-pandas.svg) 


![Cumulative Blame](https://raw.githubusercontent.com/wdm0006/git-pandas/master/examples/img/githubblame.png)

A simple set of wrappers around gitpython for creating pandas dataframes out of git data. The project is centered around
two primary objects:

 * Repository
 * ProjectDirectory
 
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
 * Approximate bus factor
 * Cumulative Blame as a time series
 * Github.com profile analysis via GitHubProfile object
 * Plotting helpers in utilities module
 * Punchcard dataframe and plotting utility
 * Filewise blame
 * File owner approximation
 * Estimation of hours spent per project or per author across projects
  
Please see examples for more detailed usage. The image above is generated using the repository object's cumulative blame
function on stravalib.


Installation
------------

Git-pandas supports python 2.7+ and 3.3+. To install use:

    pip install git-pandas
    
Documentation
-------------

Docs can be found here: [http://wdm0006.github.io/git-pandas/](http://wdm0006.github.io/git-pandas/)

Contributing
------------

We are looking for contributors, so if you are interested, please review our contributor guidelines in CONTRIBUTING.md,
which includes some proposed starter issues, or if you have an idea of your own, send us a pull request.

Projects Using Git-Pandas
-------------------------

 * [GitNOC](https://github.com/wdm0006/gitnoc)
 * [Commit Opener](https://github.com/lbillingham/commit_opener)
 
 
License
-------

This is BSD licensed (see LICENSE.md)