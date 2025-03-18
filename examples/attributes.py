"""
.. module:: attributes.py
   :platform: Unix, Windows
   :synopsis: An example showing basic attribute access to gitpandas objects.

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

from pandas import set_option

from gitpandas.project import ProjectDirectory
from gitpandas.repository import Repository

__author__ = "willmcginnis"


def repository():
    # build an example repository object then check the attributes
    r = Repository("https://github.com/wdm0006/git-pandas.git")
    print("\nRepository Name")
    print(r.repo_name)
    print("\nRepository Branches:")
    print(r.branches())
    print("\nRepository Tags:")
    print(r.tags())
    print("\nRepository Revisions:")
    print(r.revs())
    print("\nRepository Blame:")
    print(r.blame(include_globs=["*.py"]))
    print("\nRepository Is Bare:")
    print(r.is_bare())


def project():
    # build an example project directory object then check the attributes
    p = ProjectDirectory(
        [
            "https://github.com/wdm0006/git-pandas.git",
            "https://github.com/CamDavidsonPilon/lifelines.git",
        ]
    )
    print("\nProject Directory Name")
    print(p.repo_name())
    print("\nProject Directory Branches:")
    print(p.branches())
    print("\nProject Directory Tags:")
    print(p.tags())
    print("\nProject Directory Revisions:")
    print(p.revs())
    print("\nProject Directory Blame:")
    print(p.blame())
    print("\nProject Directory Is Bare:")
    print(p.is_bare())


if __name__ == "__main__":
    set_option("display.max_rows", 500)
    set_option("display.max_columns", 500)
    set_option("display.width", 1000)
    repository()
    project()
