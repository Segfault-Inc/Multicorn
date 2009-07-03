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

import operator
import re

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
