"""
A filesystem foreign data wrapper.

This foreign data wrapper is based on StructuredDirectory, see
https://github.com/Kozea/StructuredFS.

"""

from multicorn import ForeignDataWrapper
from multicorn.fsfdw.structuredfs import StructuredDirectory
from multicorn.utils import log_to_postgres
from logging import ERROR, WARNING, DEBUG
import os


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
        self.folder_columns = [key[0] for key in
                               self.structured_directory._path_parts_properties
                               if key]
        # Assume 100 files/folder per folder 
        self.total_files = 100 ** len(pattern.split('/'))
        if self.filename_column:
            if self.filename_column not in columns:
                log_to_postgres("The filename column (%s) does not exist"
                                "in the column list" % self.filename_column,
                                ERROR,
                                "You should try to create your table with an "
                                "additional column: \n"
                                "%s character varying" % self.filename_column)
            else:
                columns.pop(self.filename_column)
        if self.content_column:
            if self.content_column not in columns:
                log_to_postgres("The content column (%s) does not exist"
                                "in the column list" % self.content_column,
                                ERROR,
                                "You should try to create your table with an "
                                "additional column: \n"
                                "%s bytea" % self.content_column)
            else:
                columns.pop(self.content_column)
        if len(self.structured_directory.properties) < len(columns):
            missing_columns = set(columns.keys()).difference(
                    self.structured_directory.properties)
            log_to_postgres("Some columns are not mapped in the structured fs",
                    WARNING, "Remove the following columns: %s "
                    % missing_columns)

    def get_rel_size(self, quals, columns):
        """Helps the planner by returning costs
        For the width, we assume 30 for every returned column, + 1 million for the content
        column.

        For the number of rows, we assume 100 files per folder.
        So, if we filter down to the last folder, thats only 100 files.

        To one level up, that's already 100^2.
        """
        # TODO: find a way to give useful stats.
        cond = self._equals_cond(quals)
        nb_total = len(self.folder_columns)
        nb_fixes = len([key for key in cond if key in
                       self.folder_columns])
        nb_rows = 100 ** (nb_total - nb_fixes)
        if self.filename_column in cond:
            nb_rows = 1
        width = len(columns) * 30
        if self.content_column in columns:
            width += 1000000
        return (nb_rows, width)

    def _equals_cond(self, quals):
        return dict((qual.field_name, unicode(qual.value)) for
                qual in quals if qual.operator == '=')

    def get_path_keys(self):
        """Return the path keys for parameterized path.

        The structured fs can manage equal filters on its directory patterns,
        so that can be used.
        """
        values = [((self.filename_column,), 1)]
        folders = self.folder_columns
        for i in range(1, len(folders) + 1):
            values.append((folders[:i], 100 ** (len(folders) - i)))
        log_to_postgres(unicode(values))
        return values

    def execute(self, quals, columns):
        """Execute method.

        The FilesystemFdw performs some optimizations based on the filesystem
        structure.

        """
        cond = self._equals_cond(quals)
        log_to_postgres(str(quals))
        if self.filename_column in cond:
            item = self.structured_directory.from_filename(
                    cond[self.filename_column])
            if item is not None and os.path.exists(item.full_filename):
                new_item = dict(item)
                if self.content_column:
                    new_item[self.content_column] = item.read()
                if self.filename_column:
                    new_item[self.filename_column] = item.filename
                yield new_item
                return
        else:
            cond.pop(self.content_column, None)
            for item in self.structured_directory.get_items(**cond):
                new_item = dict(item)
                if self.content_column and self.content_column in columns:
                    new_item[self.content_column] = item.read()
                if self.filename_column and self.filename_column in columns:
                    new_item[self.filename_column] = item.filename
                yield new_item
