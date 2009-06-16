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

def set_name(f, name):
    f.__name__ = name
    return f

operators = {
    '=':   set_name(lambda a, b: a == b, 'equals'),
    '!=':  set_name(lambda a, b: a != b, 'not_equals'),
    '>':   set_name(lambda a, b: a >  b, 'greater_than'),
    '>=':  set_name(lambda a, b: a >= b, 'greater_or_equal'),
    '<':   set_name(lambda a, b: a <  b, 'lesser_than'),
    '<=':  set_name(lambda a, b: a <= b, 'lesser_or_equal'),
    '~=':  set_name(lambda a, b: re.match(b, a), 're_match'),
    '~!=': set_name(lambda a, b: not re.match(b, a), 're_not_match'),
}

class OperatorNotAvailable(Exception):
    pass

from item import Item
