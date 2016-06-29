"""

"""

from gitpandas.utilities.plotting import plot_punchcard
from gitpandas import ProjectDirectory

g = ProjectDirectory(working_dir=[...], verbose=True)

by = None
punchcard = g.punchcard(branch='master', include_globs=['*.py'], by=by, normalize=2500)
plot_punchcard(punchcard, metric='lines', title='punchcard', by=by)





