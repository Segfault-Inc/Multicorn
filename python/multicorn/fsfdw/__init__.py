"""
A filesystem foreign data wrapper.

This foreign data wrapper is based on StructuredDirectory, see
https://github.com/Kozea/StructuredFS.

"""

from multicorn import TransactionAwareForeignDataWrapper
from multicorn.fsfdw.structuredfs import StructuredDirectory
from multicorn.utils import log_to_postgres
from logging import ERROR, WARNING
import os
import errno


class FilesystemFdw(TransactionAwareForeignDataWrapper):
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
        self.file_mode = int(options.get('file_mode', '700'), 8)
        self.structured_directory = StructuredDirectory(root_dir, pattern,
                                                        file_mode=self.file_mode)
        self.folder_columns = [key[0] for key in
                               self.structured_directory._path_parts_properties
                               if key]
        # Keep a set of files that should not be seen inside the transaction,
        # because they have "logically" been deleted, but are not yet commited
        self.invisible_files = set()
        # Keep a dictionary of updated content.
        self.updated_content = dict()
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
                            level=WARNING,
                            hint="Remove the following columns: %s " %
                            missing_columns)

    def get_rel_size(self, quals, columns):
        """Helps the planner by returning costs
        For the width, we assume 30 for every returned column, + 1 million
        for the content column.

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
        return values

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

    def items_to_dicts(self, items, columns):
        content_column = self.content_column
        filename_column = self.filename_column
        has_content = content_column and content_column in columns
        has_filename = filename_column and filename_column in columns
        for item in items:
            if item.full_filename in self.invisible_files:
                continue
            new_item = dict(item)
            if has_content:
                content = self.updated_content.get(item.full_filename, None)
                if content is None:
                    content = item.read()
                new_item[content_column] = content
            if has_filename:
                new_item[filename_column] = item.filename
            yield new_item

    def _item_from_dml(self, values):
        content = values.pop(self.content_column, None)
        filename = values.pop(self.filename_column, None)
        item_from_filename = None
        item_from_values = None
        if filename:
            item_from_filename = self.structured_directory.from_filename(
                filename)
        properties_columns = self.structured_directory.properties
        supplied_keys = set(key for (key, value) in values.items()
                            if value is not None)
        if len(supplied_keys) != 0:
            if properties_columns != supplied_keys:
                log_to_postgres("The following columns are necessary: %s" %
                                properties_columns.difference(supplied_keys),
                                level=ERROR,
                                hint="You can also insert an item by providing"
                                " only the filename and content columns")
            values = {key: str(value) for key, value in values.items()}
            item_from_values = self.structured_directory.create(**values)
        elif item_from_filename is None:
            log_to_postgres("The filename, or all pattern columns are needed.",
                            level=ERROR)
        if item_from_filename and item_from_values:
            if item_from_filename != item_from_values:
                log_to_postgres("The columns inferred from the"
                                " filename do not match the supplied columns.",
                                level=ERROR,
                                hint="Remove either the filename column"
                                " or the properties column from your "
                                " statement, or ensure they match")
        item = item_from_filename or item_from_values
        item.content = content
        return item

    def _report_pk_violation(self, item):
        log_to_postgres("Duplicate key value violates filesystem"
                        " integrity.",
                        detail="Key (%s)=(%s) already exists" %
                        (', '.join(item.keys()),
                            ', '.join(item.values())),
                        level=ERROR)

    def insert(self, values):
        item = self._item_from_dml(values)
        # Ensure that the file is created.
        try:
            item.open(shared_lock=False, fail_if='exists')
        except OSError as e:
            if e.errno == errno.EEXIST:
                self._report_pk_violation(item)
            else:
                raise
        # Keep track of it for pre_commit time.
        self.invisible_files.discard(item.full_filename)
        self.updated_content[item.full_filename] = item.content
        super(FilesystemFdw, self).insert(item)
        # Update the "generated" column values.
        return_value = dict(item)
        return_value[self.filename_column] = item.filename
        return_value[self.content_column] = item.content
        return return_value

    def update(self, oldfilename, newvalues):
        # The "oldfilename" file should exist.
        olditem = self.structured_directory.from_filename(oldfilename)
        olditem.content = self.updated_content.get(olditem.full_filename,
                                                   olditem.content)
        if not olditem.content:
            # No content yet
            olditem.content = olditem.read()
        new_filename = newvalues.get(self.filename_column, oldfilename)
        filename_changed = new_filename != oldfilename
        values = {key: (None if value is None else str(value))
                  for key, value in newvalues.items()
                  if key not in (self.filename_column, self.content_column)}
        values_changed = dict(olditem) != values
        # Check for null values in the "important" parts
        null_columns = [key for key in self.structured_directory.properties
                        if values[key] is None]
        if null_columns:
            log_to_postgres("Null value in columns (%s) are not allowed" %
                            ', '.join(null_columns),
                            level=ERROR,
                            detail="Failing row contains (%s)" %
                            ', '.join(val if val is not None else 'NULL'
                                      for val in values.values()))
        if filename_changed:
            if values_changed:
                # Keep everything to not bypass conflict detection
                values = dict(olditem.items() + values.items())
            else:
                # Keep only the filename
                values = {self.filename_column: new_filename}
        else:
            values = dict(olditem.items() + values.items())
        newitem = self._item_from_dml(values)
        newitem.content = newvalues.get(self.content_column, olditem.content)
        self.updated_content[newitem.full_filename] = newitem.content
        olditem.open(shared_lock=False, fail_if='missing')
        # Ensure that the new file doesnt exists if they are
        # not the same.
        if olditem.full_filename != newitem.full_filename:
            try:
                newitem.open(shared_lock=False, fail_if='exists')
            except OSError as e:
                if e.errno == errno.EEXIST:
                    self._report_pk_violation(newitem)
                else:
                    raise
            self.invisible_files.add(olditem.full_filename)
            self.invisible_files.discard(newitem.full_filename)
        super(FilesystemFdw, self).update(olditem, newitem)
        return_value = dict(newitem)
        return_value[self.filename_column] = newitem.filename
        return_value[self.content_column] = newitem.content
        return return_value

    def delete(self, rowid):
        item = self.structured_directory.from_filename(rowid)
        # Ensure that the file exists, and is locked.
        item.open(False, fail_if='missing')
        self.invisible_files.add(item.full_filename)
        super(FilesystemFdw, self).delete(item)

    def _post_xact_cleanup(self):
        self._init_transaction_state()
        self.invisible_files = set()
        self.structured_directory.clear_cache(only_shared=False)
        self.updated_content = {}

    def pre_commit(self):
        for operation, values in self.current_transaction_state:
            if operation == 'insert':
                values.write()
            elif operation == 'update':
                olditem, newitem = values
                if olditem.full_filename == newitem.full_filename:
                    fd = olditem.open(shared_lock=False)
                else:
                    fd = newitem.open(shared_lock=False)
                    os.unlink(olditem.full_filename)
                    self.structured_directory.clear_cache_entry(
                        olditem.full_filename)
                newitem.write(fd)
            elif operation == 'delete':
                values.remove()
                self.structured_directory.clear_cache_entry(
                    values.full_filename)
        self._post_xact_cleanup()

    def rollback(self):
        for operation, values in (
                self.current_transaction_state[::-1]):
            if operation == 'insert':
                values.remove()
            elif operation == 'update':
                old_value, new_value = values
                if new_value.full_filename != old_value.full_filename:
                    os.unlink(new_value.full_filename)
                old_value.write()
        self._post_xact_cleanup()

    @property
    def rowid_column(self):
        return self.filename_column

    def end_scan(self):
        self.structured_directory.clear_cache(only_shared=True)


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
        super(ReStructuredTextFdw, self).__init__(options, columns)

    def execute(self, quals, columns):
        items = self.get_items(quals, columns)
        keys = [(name, name[5:])  # len('rest_') == 5
                for name in columns if name.startswith('rest_')]
        if keys:
            items = self.add_meta(items, keys)
        return self.items_to_dicts(items, columns)

    def add_meta(self, items, keys):
        extract_meta = self.extract_meta
        for item in items:
            meta = extract_meta(item.full_filename)
            for column, key in keys:
                item[column] = meta.get(key)
            yield item
