"""
Purpose
-------

This fdw can be used to access metadata stored in ReStructured Text files,
in a filesystem.
The files are looked up based on a pattern, and parts of the file's path are
mapped to various columns, as well as the file's content itself.

The options are exactly the same as ``multicorn.fsfdw`` itself.

If a column name is prefixed by ''rest_'', it will not be mapped to
a part of the pattern but looked up in the metadata from the ReST document.
"""


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
