from . import ForeignDataWrapper
import statgrab


class ProcessFdw(ForeignDataWrapper):

    def execute(self, quals):
        return statgrab.sg_get_process_stats()
