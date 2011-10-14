"""A process FDW"""

from . import ForeignDataWrapper
import statgrab


class ProcessFdw(ForeignDataWrapper):
    """A foreign datawrapper for querying system stats.

    It accepts no options.
    You can define any column named after a statgrab column.
    See the statgrab documentation.

    """

    def execute(self, quals, columns):
        """quals are ignored."""
        return statgrab.sg_get_process_stats()
