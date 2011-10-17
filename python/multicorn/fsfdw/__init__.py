"""
A filesystem foreign data wrapper.

This foreign data wrapper is based on StructuredDirectory, see
https://github.com/Kozea/StructuredFS.

"""

from multicorn import ForeignDataWrapper
from multicorn.fsfdw.structuredfs import StructuredDirectory
from multicorn.utils import log_to_postgres
from logging import ERROR, WARNING


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
        self.content_column = options.get('content_column', None)
        self.filename_column = options.get('filename_column', None)
        self.structured_directory = StructuredDirectory(root_dir, pattern)
        if self.filename_column:
            if self.filename_column not in columns:
                log_to_postgres("The filename column (%s) does not exist"
                                "in the column list" % self.filename_column, ERROR,
                                "You should try to create your table with an "
                                "additional column: \n"
                                "%s character varying" % self.filename_column)
            else:
                columns.remove(self.filename_column)
        if self.content_column:
            if self.content_column not in columns:
                log_to_postgres("The content column (%s) does not exist"
                                "in the column list" % self.content_column, ERROR,
                                "You should try to create your table with an "
                                "additional column: \n"
                                "%s bytea" % self.content_column)
            else:
                columns.remove(self.content_column)
        if len(self.structured_directory.properties) < len(columns):
            missing_columns = set(columns).difference(
                    self.structured_directory.properties)
            log_to_postgres("Some columns are not mapped in the structured fs",
                    WARNING, "Remove the following columns: %s "
                    % missing_columns)



    def execute(self, quals, columns):
        """Execute method.

        The FilesystemFdw performs some optimizations based on the filesystem
        structure.

        """
        cond = dict((qual.field_name, qual.value) for
                qual in quals if qual.operator == '=')
        if self.filename_column in cond:
            item = self.structured_directory.from_filename(
                    cond[self.filename_column])
            if item is not None and item.exists():
                new_item = dict(item)
                if self.content_column:
                    new_item[self.content_column] = item.read()
                if self.filename_column:
                    new_item[self.filename_column] = item.filename
                yield new_item
                return
        cond.pop(self.content_column, None)
        for item in self.structured_directory.get_items(**cond):
            new_item = dict(item)
            if self.content_column and self.content_column in columns:
                new_item[self.content_column] = item.read()
            if self.filename_column and self.filename_column in columns:
                new_item[self.filename_column] = item.filename
            yield new_item
