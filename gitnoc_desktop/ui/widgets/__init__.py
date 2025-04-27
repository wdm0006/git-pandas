"""
GitNOC Desktop UI widgets.
"""

from .overview_tab import OverviewTab
from .code_health_tab import CodeHealthTab
from .contributors_tab import ContributorsTab
from .tags_tab import TagsTab
from .cumulative_blame_tab import CumulativeBlameTab
from .base_tab import BaseTabWidget

__all__ = [
    'BaseTabWidget',
    'OverviewTab',
    'CodeHealthTab',
    'ContributorsTab',
    'TagsTab',
]
