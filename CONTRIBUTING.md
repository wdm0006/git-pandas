Contributing Code
=================

Git-pandas is a python library for analyzing git repositories with pandas.  Our goal is twofold:

 * To make the analysis of git repositories or collections of repositories simple, fast, and pleasant.
 * To give developers interested in data analysis a simple interface to datasets that they understand and have already (git data) 
 
With that in mind, we welcome and in fact would love some help.

How to Contribute
=================

The preferred workflow to contribute to git-pandas is:

 1. Fork this repository into your own github account.
 2. Clone the fork on your account onto your local disk:
 
    $ git clone git@github.com:YourLogin/git-pandas.git
    $ cd git-pandas
    
 3. Create a branch for your new awesome feature, do not work in the master branch:
 
    $ git checkout -b new-awesome-feature
    
 4. Write some code, or docs, or tests.
 5. When you are done, submit a pull request.
 
Guidelines
==========

Git-pandas is still a very young project, but we do have a few guiding principles:

 1. Maintain feature and API parity between Repository and ProjectDirectory
 2. Write detailed docstrings in sphinx format
 3. Slow or potentially memory intensive functions should have a limit option

Running Tests
=============

Test coverage is admittedly pretty bad right now, so help out by writing tests for new code. To run the tests, use:

    $ nosetests --with-coverage
    $ coverage html
    
Easy Issues / Getting Started
=============================

There are a number of issues on the near term horizon that would be great to have help with.

 1. Diff: it would be really nice to be able to call a function with 2 revs and return the diff as a dataframe. So columns for line number, filename, path, change, author, timestamp, etc. 
 2. Docs: We have a lot of examples, but they are largely not present in the documentation.  Adding some plotting to the examples using pyplot and putting them into the docs would help out new users a lot.
 3. Docs Deployment: currently we host the docs on Github pages, which has a kind of awkward deployment process. Anyone with experience automating this (or scripting even) would be very useful.
 4. File-level tracking: it would be really cool to be able to get a dataframe of one file's history in great detail. It likely would be a subset of the existing file change history function.
 5. Cross-Branch Analytics: finding differences between different branches of a single repository. Or aggregating the  results of other functions across multiple branches.
 6. Verbose Mode: add logging in more functions when verbose is set to True.
 7. Heirarchical bus factor: what's the bus factor of a file, directory, repo, project directory, etc
 8. Language analytics: what languages do we use most, what are bus factors of those languages? Who should we hire next?