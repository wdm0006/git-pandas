"""
A little utility to figure out all of the parameter names in git-pandas, and make sure we aren't mixing up language
in different functions.

"""

import gitpandas as module
import inspect
import json

__author__ = 'willmcginnis'


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


def get_signatures(m, remove_self=True):
    if remove_self:
        excludes = ['self']
    else:
        excludes = []

    out = {}
    for key in m.keys():
        try:
            for k, v in m[key].__dict__.items():
                try:
                    out[str(key) + '.' + k] = [x for x in list(inspect.getargspec(v).args) if x not in excludes]
                except:
                    pass
        except:
            out[key] = [x for x in list(inspect.getargspec(m[key]).args) if x not in excludes]

    return out


def get_distinct_params(m):
    out = set()
    for k in m.keys():
        out.update(m[k])
    return out

if __name__ == '__main__':
    sigs = get_signatures(extract_objects(module))
    print(json.dumps(sigs, indent=4))
    print(get_distinct_params(sigs))