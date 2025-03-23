""" """

import matplotlib

matplotlib.use("Agg")  # Set the backend to Agg before importing pyplot

from definitions import GIT_PANDAS_DIR

from gitpandas import ProjectDirectory
from gitpandas.utilities.plotting import plot_punchcard

g = ProjectDirectory(working_dir=[str(GIT_PANDAS_DIR)], verbose=True)

by = None
punchcard = g.punchcard(branch="master", include_globs=["*.py"], by=by, normalize=2500)
plot_punchcard(punchcard, metric="lines", title="punchcard", by=by)
