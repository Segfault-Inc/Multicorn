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
import functools
import posixpath
import ntpath


def apply_to_result(function):
    """
    Make a decorator that applies the given ``function`` to the results.
    
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
    """Return if "string" matches "pattern"."""
    return bool(re.search(pattern, string))

def re_not_match(string, pattern):
    """Return if "string" does not match "pattern"."""
    return not re.search(pattern, string)

operators = {
    u'=': operator.eq,
    u'!=': operator.ne,
    u'>': operator.gt,
    u'>=': operator.ge,
    u'<': operator.lt,
    u'<=': operator.le,
    u'~=': re_match,
    u'~!=': re_not_match,
}

operators_rev = dict((value, key) for (key, value) in operators.items())

class OperatorNotAvailable(Exception): pass

def recursive_subclasses(class_):
    """Return all "class_" subclasses recursively."""
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
    
    def __call__(self, properties):
        """Return True if this dict of properties matches this condition."""
        return self.operator(properties[self.property_name], self.value)
    
    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__.__name__, self.property_name,
                                   self.operator, self.value)
                                

class ModificationTrackingList(list):
    """
    A list with a ``modified`` attribute that becomes True when the list changes
    
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



# backported from Python 2.6.2
def _posix_relpath(path, start=posixpath.curdir):
    """Return a relative version of a path"""

    if not path:
        raise ValueError("no path specified")

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
    """Return a relative version of a path"""

    if not path:
        raise ValueError("no path specified")
    start_list = ntpath.abspath(start).split(ntpath.sep)
    path_list = ntpath.abspath(path).split(ntpath.sep)
    if start_list[0].lower() != path_list[0].lower():
        unc_path, rest = ntpath.splitunc(path)
        unc_start, rest = ntpath.splitunc(start)
        if bool(unc_path) ^ bool(unc_start):
            raise ValueError("Cannot mix UNC and non-UNC paths "
                             "(%s and %s)" % (path, start))
        else:
            raise ValueError("path is on drive %s, start on drive %s"
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
    return join(*rel_list)


# Python 2.5 compatibility
if hasattr(os.path, 'relpath'):
    # Use the stdlib one if available (Python >=2.6)
    relpath = os.path.relpath
else:
    if os.path is posixpath:
        relpath = _posix_relpath
    elif os.path is ntpath:
        relpath = _nt_relpath
    # macpath and os2emxpath do not seem to have a relpath function

