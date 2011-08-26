# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import re
import io
import errno
import os.path

from ..item import BaseItem
from ..utils import isidentifier
from ..requests import requests, helpers
from ..requests.types import Type
from .abstract import AbstractCorn


def _tokenize_pattern(pattern):
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


class FilesystemItem(BaseItem):
    @property
    def filename(self):
        # TODO: check for ambiguities.
        values = {}
        for name in self.corn.identity_properties:
            value = strict_unicode(self[name])
            if '/' in value:
                raise ValueError('Filename property values can not contain '
                                 'a slash in Filesystem.')
            values[name] = value
        return self.corn.pattern.format(**values)

    @property
    def full_filename(self):
        return os.path.join(self.corn.root_dir, *self.filename.split('/'))


class LazyFileReader(object):
    """
    Callable that returns the content of a file, but do the reading only once.
    """
    def __init__(self, filename, encoding):
        self.filename = filename
        self.encoding = encoding
        self.content = None

    def __call__(self, item):
        if self.content is None:
            if self.encoding is None:
                mode = 'rb'
            else:
                mode = 'rt'
            with io.open(self.filename, mode, encoding=self.encoding) as file:
                self.content = file.read()
        return self.content


class FilteredFilesystem(requests.LiteralRequest):
    arg_spec = ('corn', 'parts_infos')

    @requests.self_with_attrs
    def __init__(self, corn, parts_infos):
        self.corn = corn
        self.parts_infos = parts_infos

    @requests.self_with_attrs
    def __repr__(self):
        pattern_parts = self.corn.pattern.split('/')
        predicates = []
        for depth, (fixed_name, values, part_predicate) in enumerate(
                self.parts_infos):
            predicates.append(part_predicate)
            if fixed_name is not None:
                pattern_parts[depth] = fixed_name
        return '<%s %r, %r, %r>' % (
            self.obj_type().__name__, self.corn, '/'.join(pattern_parts),
            predicates)

    @property
    @requests.self_with_attrs
    def value(self):
        return self.corn._items_with(self.parts_infos)


class Filesystem(AbstractCorn):
    """
    A simple access point that keep Python Item objects in memory.

    The values for identity properties must be hashable (usable as a
    dictionary key.)
    """

    Item = FilesystemItem

    def __init__(self, name, root_dir, pattern, encoding=None,
                 content_property='content'):
        self.root_dir = unicode(root_dir)
        self.pattern = unicode(pattern)
        self.encoding = encoding
        self.content_property = content_property
        # All of these are lists (actually tuples used as immutable lists),
        # one element for each slash-separated path part.
        #   _path_parts_re:
        #       elements are compiled re objects
        #   _path_parts_properties:
        #       elements are list (actually tuples) of property names in
        #       this path part.
        self._path_parts_re, self._path_parts_properties = \
            _parse_pattern(self.pattern)

        # Flatten the list of lists
        path_properties = [prop for part in self._path_parts_properties
                                for prop in part]

        super(Filesystem, self).__init__(name, path_properties)

        for property_name in path_properties:
            self.properties[property_name] = Type(
                type=unicode, corn=self, name=property_name)

        self.properties[content_property] = Type(
            type=(bytes if encoding is None else unicode),
            corn=self, name=content_property)

    def register(self, name, **kwargs):
        raise TypeError('Filesystem does not take extra properties.')

    def save(self, item):
        filename = item.full_filename

        # Sanity checks
        assert item.corn is self
        assert filename.startswith(self.root_dir)

        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)

        if self.encoding is None:
            mode = 'wb'
        else:
            mode = 'wt'
        with io.open(filename, mode, encoding=self.encoding) as stream:
            stream.write(item[self.content_property])

    def delete(self, item):
        assert item.corn is self
        filename = item.full_filename
        assert filename.startswith(self.root_dir)

        os.remove(filename)

        # Remove empty directories up to (but not including) root_dir
        path_parts = item.filename.split('/')
        path_parts.pop()  # Last part is the file name, only keep directories.
        while path_parts:
            directory = self._join(path_parts)
            if os.listdir(directory):
                break
            else:
                os.rmdir(directory)
            path_parts.pop()  # Go to the parent directory

    def execute(self, request):
        return self._transform_request(request).execute()

    def _transform_request(self, request):
        chain = requests.as_chain(request)
        assert isinstance(chain[0], requests.StoredItemsRequest)
        storeditems_req = chain[0]
        assert object.__getattribute__(storeditems_req, 'storage') is self

        if len(chain) > 1 and isinstance(chain[1], requests.FilterRequest):
            replaced_req = filter_req = chain[1]
            predicate = object.__getattribute__(filter_req, 'predicate')
        else:
            replaced_req = storeditems_req
            predicate = requests.literal(True)

        parts_infos, remaining_predicate = self._split_predicate(predicate)
        filtered_items = FilteredFilesystem(self, parts_infos)
        replacement_req = filtered_items.filter(remaining_predicate)
        if replaced_req is request:
            return replacement_req
        else:
            return request._copy_replace({replaced_req: replacement_req})

    def _split_predicate(self, predicate):
        """
        Return a (parts_infos, remaining_predicate) tuple.

        parts_infos is a list of (fixed_name, values, predicate) or
        (None, None, predicate) tuples, one for each path part.
        For each of these tuple:
        * fixed_name and values are None unless all properties in that part
          have a fixed value with a `==` predicate.
        * fixed_name is the path part as would be returned by os.listdir()
        * values is a dict of {property name: value}.
        * predicate is what remains after removing the `==` predicate parts
          encoded in values.

        remaining_predicate is the predicate part about non-path properties.

        Joining all predicate parts in this result with `&` would give
        something equivalent to the given predicate.

        Example:
            self.pattern == 'a_{foo}/b_{bar}
            predicate == ((c.foo == 'lorem') & (c.bar != 'ipsum') &
                          (c.content == 'dolor'))

            Returns:
                (
                    [
                        ('a_lorem', {'foo': 'lorem'}, literal(True)),
                        (None, None, c.bar != 'ipsum')
                    ],
                    (c.content == 'dolor')
                )
        """
        contexts = (self.type,)
        parts_infos = []
        types = []
        remaining_predicate = self.RequestWrapper.from_request(predicate)
        for pattern_part, part_properties in zip(
                self.pattern.split('/'), self._path_parts_properties):
            # Include properties for all parts so far
            types += [self.properties[name] for name in part_properties]
            # Isolate the predicate only about this part’s properties.
            part_predicate, remaining_predicate = helpers.inner_split(
                remaining_predicate, types, contexts)
            values, non_fixed_part_predicate = helpers.isolate_values(
                part_predicate.wrapped_request, contexts)

            if all(prop in values for prop in part_properties):
                # TODO: check for ambiguities
                name = pattern_part.format(**values)
                # TODO: check non_fixed_part_predicate here?
                parts_infos.append((name, values, non_fixed_part_predicate))
            else:
                parts_infos.append((None, None,
                                    part_predicate.wrapped_request))
        return parts_infos, remaining_predicate.wrapped_request

    def _items_with(self, parts_infos):
        return self._walk([], [], parts_infos)

    def _walk(self, previous_path_parts, previous_values, parts_infos):
        # Empty path_parts means look in root_dir, depth = 0
        depth = len(previous_path_parts)
        # If the pattern has N path parts, "leaf" files are at depth = N-1
        is_leaf = (depth == len(self._path_parts_re) - 1)
        # Names of properties that are in this path part
        properties = self._path_parts_properties[depth]

        for name, values in self._find_matching_names(
                previous_path_parts, previous_values, parts_infos):
            path_parts = previous_path_parts + [name]

            if is_leaf:
                filename = self._join(path_parts)
                if os.path.isfile(filename):
                    yield self._create_item(filename, values)
            # Do not check if filename is a directory or even exists,
            # let listdir() raise later.
            else:
                for item in self._walk(path_parts, values, parts_infos):
                    yield item

    def _find_matching_names(self, previous_path_parts, previous_values,
                             parts_infos):
        depth = len(previous_path_parts)

        fixed_name, fixed_values, predicate = parts_infos[depth]
        if fixed_name is not None:
            values = dict(previous_values)
            values.update(fixed_values)
            if predicate.execute((values,)):
                yield fixed_name, values
        else:
            try:
                names = self._listdir(previous_path_parts)
            except OSError, exc:
                if exc.errno in [errno.ENOENT, errno.ENOTDIR]:
                    # Does not exist or is not a directory, just return
                    # without yielding any name.
                    return
                else:
                    # Re-raise other errors
                    raise
            for name in names:
                new_values = self._match_part(depth, name)
                if new_values is not None:
                    # name matches the pattern
                    # Make values a new copy on each iteration
                    values = dict(previous_values)
                    values.update(new_values)
                    if predicate.execute((values,)):
                        # name also matches the predicate.
                        yield name, values

    def _listdir(self, path_parts):
        # Make this monkey-patchable for tests
        # os.listdir()’s argument is unicode, so should be its results.
        return os.listdir(self._join(path_parts))

    def _join(self, path_parts):
        # root_dir is unicode, so the join result should be unicode
        return os.path.join(self.root_dir, *path_parts)

    def _match_part(self, depth, name):
        match = self._path_parts_re[depth].match(name)
        if match is None:
            return None
        else:
            return match.groupdict()

    def _create_item(self, filename, values):
        reader = LazyFileReader(filename, self.encoding)
        lazy_values = {self.content_property: reader}
        return self.create(values, lazy_values)
