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
    return bool(re.match(pattern, string))

def re_not_match(string, pattern):
    """Return if "string" does not match "pattern"."""
    return not re.match(pattern, string)

operators = {
    '=': operator.eq,
    '!=': operator.ne,
    '>': operator.gt,
    '>=': operator.ge,
    '<': operator.lt,
    '<=': operator.le,
    '~=': re_match,
    '~!=': re_not_match,
}

class OperatorNotAvailable(Exception): pass

def recursive_subclasses(class_):
    """Return all "class_" subclasses recursively"""
    yield class_
    for subclass in class_.__subclasses__():
        for sub_subclass in recursive_subclasses(subclass):
            yield sub_subclass

