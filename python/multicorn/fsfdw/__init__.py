"""
A filesystem foreign data wrapper.

This foreign data wrapper is based on StructuredDirectory, see
https://github.com/Kozea/StructuredFS.

"""

from multicorn import ForeignDataWrapper
from multicorn.fsfdw.structuredfs import StructuredDirectory
from multicorn.utils import log_to_postgres
from logging import ERROR, WARNING
import os


class FilesystemFdw(ForeignDataWrapper):
    """A filesystem foreign data wrapper.

    The foreign data wrapper accepts the following options:

    root_dir            --  The base dir for searching the file
    pattern             --  The pattern for looking for file, starting from the
                            root_dir. See :class:`StructuredDirectory`.
    content_column      --  The column's name which contains the file content.
                            (defaults to None)
    filename_column     --  The column's name wich contains the full filename.

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

    def execute(self, quals, columns):
        """Execute method.

        The FilesystemFdw performs some optimizations based on the filesystem
        structure.

        """
        return self.items_to_dicts(self.get_items(quals, columns), columns)

    def get_items(self, quals, columns):
        filename_column = self.filename_column
        for qual in quals:
            if qual.field_name == filename_column and qual.operator == '=':
                item = self.structured_directory.from_filename(
                    unicode(qual.value))
                if item is not None and os.path.exists(item.full_filename):
                    return [item]
                else:
                    return []
        properties = self.structured_directory.properties
        return self.structured_directory.get_items(**dict(
            (qual.field_name, unicode(qual.value)) for qual in quals
            if qual.operator == '=' and qual.field_name in properties))


    def items_to_dicts(self, columns, items):
        content_column = self.content_column
        filename_column = self.filename_column
        has_content = content_column and content_column in columns
        has_filename = filename_column and filename_column in columns
        for item in items:
            new_item = dict(item)
            if has_content:
                new_item[content_column] = item.read()
            if has_filename:
                new_item[filename_column] = item.filename
            yield new_item


class ReStructuredTextFdw(FilesystemFdw):
    """A filesystem with reStructuredText metadata foreign data wrapper.

    The foreign data wrapper accepts the same options as FilesystemFdw.
    Any column with a name in rest_* is set to the metadata value with the
    corresponding key. (Eg. rest_title is set to the title of the document.)

    """
    def __init__(self, options, columns):
        from multicorn.fsfdw.docutils_meta import mtime_lru_cache, extract_meta
        # TODO: make max_size configurable?
        self.extract_meta = mtime_lru_cache(extract_meta, max_size=1000)
        columns = dict((name, column) for name, column in columns.items()
                       if not name.startswith('rest_'))
        super(RestructuredText, self).__init__(options, columns)

    def execute(self, quals, columns):
        items = self.get_items(quals, columns)
        keys = [(name, name[:5])  # len('rest_') == 5
                for name in columns if name.startswith('rest_')]
        if keys:
            items = self.add_meta(items, keys)
        return self.items_to_dicts(items, columns)

    def add_meta(self, items, keys):
        extract_meta = self.extract_meta
        for item in items:
            meta = extract_meta(item.full_filename)
            item = dict(item)
            for column, key in keys:
                item[column] = meta.get(key)
            yield item
