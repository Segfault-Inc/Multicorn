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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

"""
TODO English doc
"""

import re

def           equals(a, b): return a == b
def       not_equals(a, b): return a != b
def     greater_than(a, b): return a >  b
def greater_or_equal(a, b): return a >= b
def      lesser_than(a, b): return a <  b
def  lesser_or_equal(a, b): return a <= b
def         re_match(a, b): return bool(re.match(b, a))
def     re_not_match(a, b): return  not re.match(b, a)



operators = {
    '=':   equals,
    '!=':  not_equals,
    '>':   greater_than,
    '>=':  greater_or_equal,
    '<':   lesser_than,
    '<=':  lesser_or_equal,
    '~=':  re_match,
    '~!=': re_not_match,
}

class OperatorNotAvailable(Exception):
    pass

from item import Item
