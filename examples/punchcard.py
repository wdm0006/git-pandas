"""

"""

from gitpandas.utilities.plotting import plot_punchcard
from gitpandas import ProjectDirectory

repo = ProjectDirectory(working_dir=[
    'git://github.com/wdm0006/git-pandas.git',
    'git://github.com/wdm0006/categorical_encoding.git',
    'git://github.com/wdm0006/sklearn-extensions.git',
    'git://github.com/wdm0006/pygeohash.git',
    'git://github.com/wdm0006/petersburg.git',
    'git://github.com/wdm0006/incomprehensible.git',
], verbose=True)

by = None
punchcard = repo.punchcard(branch='master', extensions=['py'], by=by, normalize=2500)
plot_punchcard(punchcard, metric='lines', title='punchcard', by=by)
