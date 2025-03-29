"""
.. module:: utilities
   :platform: Unix, Windows
   :synopsis: Helper methods for plotting or otherwise manipulating output from gitpandas objects

.. moduleauthor:: Will McGinnis <will@pedalwrencher.com>


"""

__author__ = "willmcginnis"
__version__ = "2.1.0"

try:
    import joblib  # noqa: F401

    _has_joblib = True
except ImportError:
    _has_joblib = False
