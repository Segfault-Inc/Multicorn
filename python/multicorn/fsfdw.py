from . import ForeignDataWrapper
from structuredfs import StructuredDirectory


class FilesystemFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(FilesystemFdw, self).__init__(options)
        root_dir = options.get('root_dir')
        pattern = options.get('pattern')
        self.sd = StructuredDirectory(root_dir, pattern)

    def execute(self, quals):
        return self.sd.get_items()
