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
Kalamar request objects.

"""

import operator
import re


OPERATORS = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "~=": re_match,
    "~!=": re_not_match,
    "AND": operator.and_,
    "OR": operator.or_}
REVERSE_OPERATORS = dict((value, key) for key, value in operators.items())


class OperatorNotAvailable(ValueError):
    """Operator unavailable."""


class Request(object):
    """Container for ``(left_operand, operator, right_operand)``."""
    def __init__(self, left_operand, operator, right_operand):
        self.left_operand = left_operand
        self.operator = operator
        self.right_operand = right_operand

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__, self.property_name, self.operator,
            self.value)


class Condition(Request):
    """Container for ``(property_name, operator, value)``."""
    def __init__(self, property_name, operator, value):
        super(Condition, self).__init__(property_name, operator, value)
        self.property_name = property_name
        self.value = value
    

class And(Request):
    """Container for ``(first, and, (second, and, (...)))``."""
    def __init__(self, values):
        values = list(values)
        super(And, self).__init__(values.pop(), OPERATORS["AND"], None)
        if len(values) > 1:
            self.right_operand = And(values)
        else:
            self.right_operand = values.pop()


class Or(Request):
    """Container for ``(first, or, (second, or, (...)))``."""
    def __init__(self, values):
        values = list(values)
        super(Or, self).__init__(values.pop(), OPERATORS["OR"], None)
        if len(values) > 1:
            self.right_operand = Or(values)
        else:
            self.right_operand = values.pop()
