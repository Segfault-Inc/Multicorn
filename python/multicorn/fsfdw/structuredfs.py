"""

Handle nicely a set of files in a structured directory.

"""


import os
import re
import errno
import string
import collections


vformat = string.Formatter().vformat


try:
    str.isidentifier
except AttributeError:
    # Python 2
    # http://docs.python.org/py3k/reference/lexical_analysis.html#identifiers
    # the uppercase and lowercase letters A through Z, the underscore _
    # and, except for the first character, the digits 0 through 9.
    _IDENTIFIERS_RE = re.compile('^[a-zA-Z_][a-zA-Z_0-9]*$')

    def isidentifier(string):
        """
        Return whether the given string is a valid Python identifier.
        """
        return _IDENTIFIERS_RE.match(string) is not None
else:
    # Python 3
    def isidentifier(string):
        """
        Return whether the given string is a valid Python identifier.
        """
        return string.isidentifier()


def _tokenize_pattern(pattern):
    """
    Return an iterable of tokens from a string pattern.

    >>> list(_tokenize_pattern('{category}/{number}_{name}.txt'))
    [('property', 'category'),
     ('path separator', '/'),
     ('category', 'number'),
     ('literal', '_'),
     ('category', 'name'),
     ('path separator', '/')]

    """
    # We could re-purpose the parser for str.format() and use string.Formatter,
    # but we do not want to parse conversions and format specs.
    in_field = False
    field_name = None
    char_list = list(pattern)
    for prev_char, char, next_char in zip(
            [None] + char_list[:-1],
            char_list,
            char_list[1:] + [None]):
        if in_field:
            if char == '}':
                yield 'property', field_name
                field_name = None
                in_field = False
            else:
                field_name += char
        else:
            if char == '/':
                yield 'path separator', char
            elif char in '{}' and next_char == char:
                # Two brakets are parsed as one. Ignore the first one.
                pass
            elif char == '}' and prev_char != char:
                raise ValueError("Single '}' encountered in format string")
            elif char == '{' and prev_char != char:
                in_field = True
                field_name = ''
            else:
                # Includes normal chars but also an escaped bracket.
                yield 'literal', char
    if in_field:
        raise ValueError("Unmatched '{' in format string")

    # Artificially add this token to simplify the parser below
    yield 'path separator', '/'


def _parse_pattern(pattern):
    r"""
    Parse a string pattern and return (path_parts_re, path_parts_properties)

    >>> _parse_pattern('{category}/{number}_{name}.txt')
    (
        (
            <re object '^(?P<category>.*)$'>,
            <re object r'^(?P<number>.*)\_(?P<name>.*)\.txt$'>
        ), (
            ('category',)
            ('number', 'name')
        ),
    )
    """
    # A list of list of names
    path_parts_properties = []
    # The next list of names, being built
    properties = []
    # A set of all names
    all_properties = set()

    # A list of compiled re objects
    path_parts_re = []
    # The pattern being built for the next re
    next_re = ''

    for token_type, token in _tokenize_pattern(pattern):
        if token_type == 'path separator':
            if not next_re:
                raise ValueError('A slash-separated part is empty in %r' %
                                 pattern)
            path_parts_re.append(re.compile('^%s$' % next_re))
            next_re = ''
            path_parts_properties.append(tuple(properties))
            properties = []
        elif token_type == 'property':
            if not isidentifier(token):
                raise ValueError('Invalid property name for Filesystem: %r. '
                                 'Must be a valid identifier' % token)
            if token in all_properties:
                raise ValueError('Property name %r appears more than once '
                                 'in the pattern %r.' % (token, pattern))
            all_properties.add(token)
            properties.append(token)
            next_re += '(?P<%s>.*)' % token
        elif token_type == 'literal':
            next_re += re.escape(token)
        else:
            assert False, 'Unexpected token type: ' + token_type

    # Always end with an artificial '/' token so that the last regex is
    # in path_parts_re.
    assert token_type == 'path separator'

    return tuple(path_parts_re), tuple(path_parts_properties)


def strict_unicode(value):
    """
    Make sure that value is either unicode or (on Py 2.x) an ASCII string,
    and return it in unicode. Raise otherwise.
    """
    if not isinstance(value, basestring):
        raise TypeError('Filename property values must be of type '
                        'unicode, got %r.' % value)
    return unicode(value)


class Item(collections.Mapping):
    """
    Represents a single file in a :class:`StructuredDirectory`.

    Can be used as a read-only mapping (dict-like) to access properties.

    Note that at a given point in time, the actual file for an Item may or
    may not exist in the filesystem.
    """
    def __init__(self, directory, properties):
        properties = dict(properties)
        keys = set(properties)
        missing = directory.properties - keys
        if missing:
            raise ValueError('Missing properties: %s', ', '.join(missing))
        extra = keys - directory.properties
        if extra:
            raise ValueError('Unknown properties: %s', ', '.join(extra))

        self.directory = directory
        self._properties = {}
        # TODO: check for ambiguities.
        # eg. with pattern = '{a}_{b}', values {'a': '1_2', 'b': '3'} and
        # {'a': '1', 'b': '2_3'} both give the same filename.
        for name, value in properties.items():
            value = strict_unicode(value)
            if '/' in value:
                raise ValueError('Property values can not contain a slash.')
            self._properties[name] = value

    @property
    def filename(self):
        """
        Return the normalized (slash-separated) filename for the item,
        relative to the root.
        """
        return vformat(self.directory.pattern, [], self)

    @property
    def full_filename(self):
        """
        Return the absolute filename for the item, in OS-specific form.
        """
        return self.directory._join(self.filename.split('/'))

    def read(self):
        """
        Return the content of the file as a bytestring.

        :raises IOError: if the file does not exist in the filesystem.
        """
        with open(self.full_filename, 'rb') as file_:
            return file_.read()

    def write(self, content):
        """
        Create or overwrite the file with the ``content`` bytestring.
        """
        filename = self.full_filename
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(filename, 'wb') as file_:
            file_.write(content)

    def remove(self):
        """
        Remove the file from the filesystem.

        :raises OSError: if the file does not exist in the filesystem.
        """
        os.remove(self.full_filename)

        # Remove empty directories up to (but not including) root_dir
        path_parts = self.filename.split('/')
        path_parts.pop()  # Last part is the file name, only keep directories.
        while path_parts:
            directory = self.directory._join(path_parts)
            if os.listdir(directory):
                break
            else:
                os.rmdir(directory)
            path_parts.pop()  # Go to the parent directory

    # collections.Mapping interface:

    def __len__(self):
        return len(self._properties)

    def __iter__(self):
        return iter(self._properties)

    def __getitem__(self, name):
        return self._properties[name]


class StructuredDirectory(object):
    """
    :param root_dir: Path to the root directory
    :param pattern: Pattern for files in this directory,
                    eg. '{category}/{number}_{name}.txt'
    """
    def __init__(self, root_dir, pattern):
        self.root_dir = unicode(root_dir)
        self.pattern = unicode(pattern)

        parts_re, parts_properties = _parse_pattern(self.pattern)
        self._path_parts_re = parts_re
        self._path_parts_properties = parts_properties
        self.properties = set(prop for part in parts_properties
                                   for prop in part)

    def get_items(self, **fixed_values):
        """
        Return an iterable of :class:`Item` objects for all files
        that match the given ``properties``.
        """
        keys = set(fixed_values)
        extra = keys - self.properties
        if extra:
            raise ValueError('Unknown properties: %s', ', '.join(extra))

        # Pre-compute everything we know about the request without looking
        # at the filesystem.

        # `fixed` is a list, one element for each "part" of the pattern.
        # Each element is a (fixed_part, fixed_part_values) tuple.
        #    fixed_part: the whole part if it is completly fixed, or None
        #    fixed_part_values: (name, value) pairs for$ fixed values
        #                       for this part.
        fixed = []
        for pattern_part, part_properties in zip(
                self.pattern.split('/'), self._path_parts_properties):
            fixed_part_values = tuple(
                (name, fixed_values[name]) for name in part_properties
                if name in fixed_values
            )
            if len(fixed_part_values) == len(part_properties):
                # All properties for this part are fixed
                fixed_part = vformat(pattern_part, [], dict(fixed_part_values))
            else:
                fixed_part = None
            fixed.append((fixed_part, fixed_part_values))

        return self._walk((), (), fixed)

    def _walk(self, previous_path_parts, previous_values, fixed):
        """
        Called for each directory or sub-directory.
        """
        # Empty previous_path_parts means look in root_dir, depth = 0
        depth = len(previous_path_parts)
        # If the pattern has N path parts, "leaf" files are at depth = N-1
        is_leaf = (depth == len(self._path_parts_re) - 1)

        for name, part_values in self._find_matching_names(
                previous_path_parts, fixed):
            path_parts = previous_path_parts + (name,)
            values = previous_values + tuple(part_values)
            filename = self._join(path_parts)
            if is_leaf:
                if os.path.isfile(filename):
                    yield Item(self, values)
            # Do not check if filename is a directory or even exists,
            # let listdir() raise later.
            else:
                for item in self._walk(path_parts, values, fixed):
                    yield item

    def _find_matching_names(self, previous_path_parts, fixed):
        """
        Yield names and parsed values that match the request in a directory.
        """
        depth = len(previous_path_parts)
        fixed_part, fixed_part_values = fixed[depth]
        if fixed_part is not None:
            yield fixed_part, fixed_part_values
            return

        try:
            names = self._listdir(previous_path_parts)
        except OSError, exc:
            if depth > 0 and exc.errno in [errno.ENOENT, errno.ENOTDIR]:
                # Does not exist or is not a directory, just return
                # without yielding any name.
                # If depth == 0, we're listing the root directory. Still raise
                # in that case.
                return
            else:
                # Re-raise other errors
                raise
        for name in names:
            match = self._path_parts_re[depth].match(name)
            if match is None:
                continue

            part_values = match.groupdict()
            if all(part_values[name] == value
                   for name, value in fixed_part_values):
                yield name, part_values.items()

    def _join(self, path_parts):
        """
        Return a full filesystem path from parts relative to the root.
        """
        # root_dir is unicode, so the join result should be unicode
        return os.path.join(self.root_dir, *path_parts)

    def _listdir(self, path_parts):
        """
        Wrap os.listdir to make it monkey-patchable in tests.
        """
        return os.listdir(self._join(path_parts))
