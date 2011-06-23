# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


import re
import string
import itertools

from ..utils import isidentifier
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
                # Includes normal chars but also a bracket after the same one.
                yield 'literal', char
    if in_field:
        raise ValueError("Unmatched '{' in format string")


def _parse_pattern(pattern):
    # Property names
    path_properties = []
    # One regular expression of each slash-separated part of the pattern
    path_parts_re = []
    next_re = ''

    for token_type, token in _tokenize_pattern(pattern):
        if token_type == 'path separator':
            if not next_re:
                raise ValueError('A slash-separated part is empty in %r' %
                                 pattern)
            path_parts_re.append('^%s$' % next_re)
            next_re = ''
        elif token_type == 'property':
            if not isidentifier(token):
                raise ValueError('Invalid property name for Filesystem: %r. '
                                 'Must be a valid identifier' % token)
            if token in path_properties:
                raise ValueError('Property name %r appears more than once '
                                 'in the pattern %r.' % (token, pattern))
            path_properties.append(token)
            next_re += '(?P<%s>.*)' % token
        elif token_type == 'literal':
            next_re += re.escape(token)
        else:
            assert False, 'Unexpected token type: ' + token_type

    if not next_re:
        raise ValueError('A slash-separated part is empty in %r' %
                         pattern)
    path_parts_re.append('^%s$' % next_re)

    return tuple(path_parts_re), tuple(path_properties)


class Filesystem(AbstractCorn):
    """
    A simple access point that keep Python Item objects in memory.

    The values for identity properties must be hashable (usable as a
    dictionary key.)
    """

    def __init__(self, name, root_dir, pattern, encoding=None,
                 content_property_name='content'):
        self.root_dir = unicode(root_dir)
        self.pattern = unicode(pattern)
        self.encoding = encoding
        self.content_property_name = content_property_name
        self._path_parts_re, path_properties = _parse_pattern(self.pattern)

        super(Filesystem, self).__init__(name, path_properties)

        for property_name in path_properties:
            self.properties[property_name] = Type(
                type=unicode, corn=self, name=name)

        self.properties[content_property_name] = Type(
            type=(bytes if encoding is None else unicode),
            corn=self, name=content_property_name)

    def register(self, name, **kwargs):
        raise TypeError('Filesystem does not take extra properties.')

    def _filename_from_item(self, item):
        # TODO: check for ambiguities.
        values = {}
        for name in self.identity_properties:
            value = item[name]
            if not isinstance(value, basestring):
                raise TypeError('Filename property values must be of type '
                                'unicode, got %r.' % value)
            value = unicode(value)
            if '/' in value:
                raise ValueError('Filename property values can not contain '
                                 'a slash in Filesystem.')
            values[name] = value
        return self.pattern.format(**values)

    def _values_from_filename(self, filename):
        values = {}
        parts = filename.split('/')
        if len(parts) != len(self._path_parts_re):
            return None
        for path_part, regex in zip(parts, self._path_parts_re):
            match = re.match(regex, path_part)
            if match is None:
                return None
            values.update(match.groupdict())
        return values
