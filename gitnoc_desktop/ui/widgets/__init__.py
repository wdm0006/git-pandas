"""UI widgets for GitNOC Desktop."""

from .base_tab import BaseTabWidget
from .code_health_tab import CodeHealthTab
from .contributors_tab import ContributorsTab
from .cumulative_blame_tab import CumulativeBlameTab
from .overview_tab import OverviewTab
from .tags_tab import TagsTab

__all__ = [
    "BaseTabWidget",
    "OverviewTab",
    "CodeHealthTab",
    "ContributorsTab",
    "TagsTab",
    "CumulativeBlameTab",
]
