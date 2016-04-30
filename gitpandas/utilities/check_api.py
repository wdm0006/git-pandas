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


def parse_docstring(ds):
    ds = [x.strip() for x in ds.split('\n')]
    ds = [x.split(':') for x in ds if x.startswith(':param')]
    ds = [{x[1].replace('param', '').strip(): x[2].strip()} for x in ds]
    return ds


def get_signatures(m, remove_self=True, include_docstring=True):
    if remove_self:
        excludes = ['self']
    else:
        excludes = []

    out = {}
    for key in m.keys():
        try:
            for k, v in m[key].__dict__.items():
                try:
                    if include_docstring:
                        out[str(key) + '.' + k] = {
                            'args': [x for x in list(inspect.getargspec(v).args) if x not in excludes],
                            'docstring': parse_docstring(v.__doc__)
                        }
                    else:
                        out[str(key) + '.' + k] = {'args': [x for x in list(inspect.getargspec(v).args) if x not in excludes]}
                except Exception:
                    pass
        except Exception:
            if include_docstring:
                out[key] = {
                    'args': [x for x in list(inspect.getargspec(m[key]).args) if x not in excludes],
                    'docstring': parse_docstring(m[key].__doc__)
                }
            else:
                out[key] = {'args': [x for x in list(inspect.getargspec(m[key]).args) if x not in excludes]}

    return out


def get_distinct_params(m):
    out = set()
    for k in m.keys():
        out.update(m[k]['args'])
    return out

if __name__ == '__main__':
    sigs = get_signatures(extract_objects(module))
    print(json.dumps(sigs, indent=4))
    print(get_distinct_params(sigs))