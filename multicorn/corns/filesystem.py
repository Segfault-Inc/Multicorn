# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import re
import io
import string
import os.path
import itertools

from ..item import BaseItem
from ..utils import isidentifier
from ..requests import requests, helpers, CONTEXT as c
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
        path_parts.pop() # Last part is the file name, only keep directories.
        while path_parts:
            directory = self._join(path_parts)
            if os.listdir(directory):
                break
            else:
                os.rmdir(directory)
            path_parts.pop()  # Go to the parent directory

    def execute(self, request):
        chain = requests.as_chain(request)
        assert isinstance(chain[0], requests.StoredItemsRequest)
        assert requests.WithRealAttributes(chain[0]).storage is self

        if len(chain) > 1 and isinstance(chain[1], requests.FilterRequest):
            filter_req = chain[1]
            wrapped_filter = self.RequestWrapper.from_request(filter_req)
            id_names = self.identity_properties
            id_types = [self.properties[name] for name in id_names]

            contexts = (wrapped_filter.subject.return_type().inner_type,)
            id_predicate, other_predicate = helpers.inner_split(
                wrapped_filter.predicate, id_types, contexts)
            known_values, non_fixed_id_predicate = helpers.isolate_values(
                id_predicate.wrapped_request, contexts)

            if known_values:
                filtered_items = self._items_with(
                    known_values, non_fixed_id_predicate)
                wrapped = self.RequestWrapper.from_request(request)
                filtered_request = requests.literal(filtered_items) \
                    .filter(non_fixed_id_predicate & other_predicate)
                new_request = wrapped._copy_replace(
                    {filter_req: filtered_request})
                return new_request.execute()
        return super(Filesystem, self).execute(request)

    def _all(self):
        return self._items_with()

    def _items_with(self, fixed_values=None, predicate=None):
        return self._walk([], [], fixed_values or {}, predicate)

    def _walk(self, path_parts, previous_values, fixed_values, predicate):
        # Empty path_parts means look in root_dir, depth = 0
        depth = len(path_parts)
        # If the pattern has N path parts, "leaf" files are at depth = N-1
        is_leaf = (depth == len(self._path_parts_re) - 1)
        # Names of properties that are in this path part
        properties = self._path_parts_properties[depth]

        names = []
        if all(prop in fixed_values for prop in properties):
            new_values = dict((prop, fixed_values[prop])
                              for prop in properties)
            name = self.pattern.split('/')[depth].format(**new_values)
            values = previous_values + new_values.items()
            names.append((name, values))
        else:
            # os.listdir()’s argument is unicode, so should be its results.
            for name in os.listdir(self._join(path_parts)):
                new_values = self._match_part(depth, name, predicate)
                if new_values is not None:
                    # name matches the pattern.
                    values = previous_values + new_values
                    names.append((name, values))

        for name, values in names:
            new_path_parts = path_parts + [name]
            filename = self._join(new_path_parts)

            if is_leaf and os.path.isfile(filename):
                yield self._create_item(filename, values)
            elif (not is_leaf) and os.path.isdir(filename):
                for item in self._walk(new_path_parts, values,
                                       fixed_values, predicate):
                    yield item

    def _join(self, path_parts):
        # root_dir is unicode, so the join result should be unicode
        return os.path.join(self.root_dir, *path_parts)

    def _match_part(self, depth, name, predicate):
        # TODO: filter out with predicate
        match = self._path_parts_re[depth].match(name)
        if match is None:
            return None
        else:
            # Use .items() to return a list of pairs instead of a dict.
            # This is also a valid contructor for a dict, and lists are
            # easier to concatenate without mutation.
            return match.groupdict().items()

    def _create_item(self, filename, values):
        reader = LazyFileReader(filename, self.encoding)
        lazy_values = {self.content_property: reader}
        return self.create(values, lazy_values)
