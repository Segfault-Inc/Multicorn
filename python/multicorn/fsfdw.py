"""
A filesystem foreign data wrapper.

This foreign data wrapper is based on StructuredDirectory, see
https://github.com/Kozea/StructuredFS.

"""

from . import ForeignDataWrapper
from structuredfs import StructuredDirectory


class FilesystemFdw(ForeignDataWrapper):
    """A filesystem foreign data wrapper.

    The foreign data wrapper accepts the following options:

    root_dir            --  The base dir for searching the file
    pattern             --  The pattern for looking for file, starting from the
                            root_dir. See :class:`StructuredDirectory`.
    content_property    --  The column's name which contains the file content.
                            (defaults to None)
    filename_property   --  The column's name wich contains the full filename.

    """

    def __init__(self, options, columns):
        super(FilesystemFdw, self).__init__(options, columns)
        root_dir = options.get('root_dir')
        pattern = options.get('pattern')
        self.content_property = options.get('content_property', None)
        self.filename_property = options.get('filename_property', None)
        self.structured_directory = StructuredDirectory(root_dir, pattern)

    def execute(self, quals):
        """Execute method.

        The FilesystemFdw performs some optimizations based on the filesystem
        structure.

        """
        cond = dict((qual.field_name, qual.value) for
                qual in quals if qual.operator == '=')
        for item in self.structured_directory.get_items(**cond):
            new_item = dict(item)
            if self.content_property:
                new_item[self.content_property] = item.read()
            if self.filename_property:
                new_item[self.filename_property] = item.filename
            yield new_item
