"""
A little utility to figure out all of the parameter names in git-pandas, and make sure we aren't mixing up language
in different functions.

"""

import inspect

__author__ = "willmcginnis"


def extract_objects(m, classes=True, functions=False):
    # add in the classes at this level
    out = {}
    if classes:
        m_dict = {k: v for k, v in m.__dict__.items() if inspect.isclass(v)}
        out.update(m_dict)
    if functions:
        m_dict = {k: v for k, v in m.__dict__.items() if inspect.isfunction(v)}
        out.update(m_dict)

    return out


def parse_docstring(ds):
    ds = [x.strip() for x in ds.split("\n")]
    ds = [x.split(":") for x in ds if x.startswith(":param")]
    ds = [{x[1].replace("param", "").strip(): x[2].strip()} for x in ds]
    return ds


def get_distinct_params(m):
    out = set()
    for k in m:
        out.update(m[k]["args"])
    return out


if __name__ == "__main__":
    print("Development utilities for analyzing the git-pandas API")
