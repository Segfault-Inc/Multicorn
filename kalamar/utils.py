# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Kalamar various utils.

"""

import os.path
import operator
import re
import hashlib
import functools
import posixpath
import ntpath
import werkzeug

def apply_to_result(function):
    """Make a decorator that applies ``function`` to the results.
    
    >>> @apply_to_result(list)
    ... def foo():
    ...     "A generator"
    ...     yield 'bar'
    >>> foo.__doc__
    'A generator'
    >>> foo()
    ['bar']

    """
    def _decorator(f):
        @functools.wraps(f)
        def _decorated(*args, **kwargs):
            return function(f(*args, **kwargs))
        return _decorated
    return _decorator

def re_match(string, pattern):
    """Return if ``string`` matches ``pattern``."""
    return bool(re.search(pattern, string))

def re_not_match(string, pattern):
    """Return if ``string" does not match ``pattern``."""
    return not re.search(pattern, string)

operators = {
    u'=': operator.eq,
    u'!=': operator.ne,
    u'>': operator.gt,
    u'>=': operator.ge,
    u'<': operator.lt,
    u'<=': operator.le,
    u'~=': re_match,
    u'~!=': re_not_match}

operators_rev = dict((value, key) for (key, value) in operators.items())

class OperatorNotAvailable(ValueError): pass
class ParserNotAvailable(ValueError): pass

def recursive_subclasses(class_):
    """Return all ``class_`` subclasses recursively."""
    yield class_
    for subclass in class_.__subclasses__():
        for sub_subclass in recursive_subclasses(subclass):
            yield sub_subclass

class Condition(object):
    """A contener for property_name, operator, value."""
    def __init__(self, property_name, operator, value):
        self.property_name = property_name
        self.operator = operator
        self.value = value
    
    def __call__(self, item):
        """Return True if this dict of properties matches this condition."""
        return self.operator(item[self.property_name], self.value)
    
    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__.__name__, self.property_name,
                                   self.operator, self.value)
                                

class ModificationTrackingList(list):
    """List with a ``modified`` attribute becoming True when the list changes.
    
    >>> l = ModificationTrackingList(range(3))
    >>> l
    ModificationTrackingList([0, 1, 2])
    >>> l.modified
    False
    >>> l.pop()
    2
    >>> l
    ModificationTrackingList([0, 1])
    >>> l.modified
    True
    
    """
    
    def __init__(self, *args, **kwargs):
        super(ModificationTrackingList, self).__init__(*args, **kwargs)
        self.modified = False
    
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))
    
    def modifies(name):
        def oncall(self, *args, **kw):
            self.modified = True
            return getattr(super(ModificationTrackingList, self),
                           name)(*args, **kw)
        oncall.__name__ = name
        return oncall
    
    __delitem__ = modifies('__delitem__')
    __delslice__ = modifies('__delslice__')
    __iadd__ = modifies('__iadd__')
    __imul__ = modifies('__imul__')
    __setitem__ = modifies('__setitem__')
    __setslice__ = modifies('__setslice__')
    append = modifies('append')
    extend = modifies('extend')
    insert = modifies('insert')
    pop = modifies('pop')
    remove = modifies('remove')
    reverse = modifies('reverse')
    sort = modifies('sort')
   
    del modifies


class AliasedMultiDict(object):
    """MultiDict-like class using aliased keys.

    AliasedMultiDict is like a MultiDict, but using a dictionary of aliases
    available as AliasedMultiDict keys (in addition of the standard MultiDict
    keys).

    >>> aliases = {'alias1': 'key1', 'alias2': 'key2'}
    >>> data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
    >>> aliasedmultidict = AliasedMultiDict(data, aliases)

    >>> aliasedmultidict['key1']
    'value1'
    >>> aliasedmultidict['alias1']
    'value1'
    >>> aliasedmultidict['key3']
    'value3'

    Note that:
    >>> issubclass(AliasedMultiDict, werkzeug.MultiDict)
    False

    """
    # TODO: use the MultiDict power by coding getlist/setlist (or not?)
    def __init__(self, data, aliases):
        self.data = data
        self.aliases = aliases

    @werkzeug.cached_property
    def reversed_aliases(self):
        return dict((v,k) for k,v in self.aliases.iteritems())
    
    # Sized
    def __len__(self):
        return len(self.keys())

    # Container
    def __contains__(self, key):
        key = self.aliases.get(key, key)
        return key in self.data

    # Iterable
    def __iter__(self):
        for key in self.data:
            yield self.reversed_aliases.get(key, key)

    # Mapping
    def __getitem__(self, key):
        key = self.aliases.get(key, key)
        return self.data[key]

    def get(self, key, default=None):
        # copied from Python 2.6.4’s _abcoll module
        try:
            return self[key]
        except KeyError:
            return default
        
    def iterkeys(self):
        # copied from Python 2.6.4’s _abcoll module
        return iter(self)

    def itervalues(self):
        return self.data.itervalues()

    def iteritems(self):
        for key, value in self.data.iteritems():
            key = self.reversed_aliases.get(key, key)
            yield (key, value)

    def keys(self):
        # copied from Python 2.6.4’s _abcoll module
        return list(iter(self))

    def items(self):
        return list(self.iteritems())

    def values(self):
        return self.data.values()

    def __eq__(self, other):
        # adapted from Python 2.6.4’s _abcoll module
        return isinstance(other, AliasedMultiDict) and \
               dict(self.data) == dict(other.data)

    def __ne__(self, other):
        # copied from Python 2.6.4’s _abcoll module
        return not (self == other)

    # MutableMapping
    def __setitem__(self, key, value):
        key = self.aliases.get(key, key)
        self.data[key] = value

    def __delitem__(self, key):
        key = self.aliases.get(key, key)
        self.data[key] = value

    __marker = object()
    def pop(self, key, default=__marker):
        # copied from Python 2.6.4’s _abcoll module
        try:
            value = self[key]
        except KeyError:
            if default is self.__marker:
                raise
            return default
        else:
            del self[key]
            return value

    def popitem(self):
        # copied from Python 2.6.4’s _abcoll module
        try:
            key = next(iter(self))
        except StopIteration:
            raise KeyError
        value = self[key]
        del self[key]
        return key, value

    def clear(self):
        # copied from Python 2.6.4’s _abcoll module
        try:
            while True:
                self.popitem()
        except KeyError:
            pass

    def update(self, other=(), **kwds):
        # adapted from Python 2.6.4’s _abcoll module
        if hasattr(other, "keys"):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value
        for key, value in kwds.items():
            self[key] = value

    def setdefault(self, key, default=None):
        # copied from Python 2.6.4’s _abcoll module
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

def simple_cache(function):
    """Decorator that caches function results.

    The key used is a hash of the ``repr()`` of all arguments. The cache dict
    is accessible as the ``cache`` attribute of the decorated function.
    
    Warning: the results stay in memory until the decorated function is
    garbage-collected or you explicitly remove them.
    
    TODO: Maybe automatially remove results that weren’t used for a long time?
    
    >>> @simple_cache
    ... def f():
    ...     print 'Computing the answer...'
    ...     return 42
    >>> f()
    Computing the answer...
    42
    >>> f()
    42
    >>> f.cache # doctest: +ELLIPSIS
    {'...': 42}
    >>> f.cache.clear()
    >>> f.cache
    {}

    """
    cache = {}
    @functools.wraps(function)
    def _wrapped(*args, **kwargs):
        key = hashlib.md5(repr((args, kwargs))).digest()
        try:
            return cache[key]
        except KeyError:
            val = function(*args, **kwargs)
            cache[key] = val
        return val
    _wrapped.cache = cache
    return _wrapped

# Python 2.5 compatibility
try:
    # Use the stdlib one if available (Python >=2.6)
    from os.path import relpath
except ImportError:
    # backported from Python 2.6.2
    def _posix_relpath(path, start=posixpath.curdir):
        """Return a relative version of a path."""
        if not path:
            raise ValueError("No path specified")

        start_list = posixpath.abspath(start).split(posixpath.sep)
        path_list = posixpath.abspath(path).split(posixpath.sep)

        # Work out how much of the filepath is shared by start and path.
        i = len(posixpath.commonprefix([start_list, path_list]))

        rel_list = [posixpath.pardir] * (len(start_list)-i) + path_list[i:]
        if not rel_list:
            return posixpath.curdir
        return posixpath.join(*rel_list)

    # backported from Python 2.6.2
    def _nt_relpath(path, start=ntpath.curdir):
        """Return a relative version of a path."""
        if not path:
            raise ValueError("No path specified")
        start_list = ntpath.abspath(start).split(ntpath.sep)
        path_list = ntpath.abspath(path).split(ntpath.sep)
        if start_list[0].lower() != path_list[0].lower():
            unc_path, rest = ntpath.splitunc(path)
            unc_start, rest = ntpath.splitunc(start)
            if bool(unc_path) ^ bool(unc_start):
                raise ValueError("Cannot mix UNC and non-UNC paths "
                                 "(%s and %s)" % (path, start))
            else:
                raise ValueError("Path is on drive %s, start on drive %s"
                                 % (path_list[0], start_list[0]))

        # Work out how much of the filepath is shared by start and path.
        for i in range(min(len(start_list), len(path_list))):
            if start_list[i].lower() != path_list[i].lower():
                break
        else:
            i += 1

        rel_list = [ntpath.pardir] * (len(start_list)-i) + path_list[i:]
        if not rel_list:
            return ntpath.curdir
        return ntpath.join(*rel_list)

    if os.path is posixpath:
        relpath = _posix_relpath
    elif os.path is ntpath:
        relpath = _nt_relpath
    # macpath and os2emxpath do not seem to have a relpath function

