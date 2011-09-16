from . import ForeignDataWrapper
from structuredfs import StructuredDirectory


class FilesystemFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(FilesystemFdw, self).__init__(options)
        root_dir = options.get('root_dir')
        pattern = options.get('pattern')
        self.content_property = options.get('content_property', None)
        self.filename_property = options.get('filename_property', None)
        self.sd = StructuredDirectory(root_dir, pattern);

    def execute(self, quals):
        cond = dict((qual.field_name, qual.value) for
                qual in quals if qual.operator == '=')
        for item in self.sd.get_items(**cond):
            new_item = dict(item)
            if self.content_property:
                new_item[self.content_property] = item.read()
            if self.filename_property:
                new_item[self.filename_property] = item.filename
            yield new_item
