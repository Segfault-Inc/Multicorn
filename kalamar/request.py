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
from itertools import group_by

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

    def test(self, item):
        """Return if :prop:`item` matches the request."""
        left_operand = self.left_operand.test(item) \
            if isinstance(self.left_operand, Request) else self.left_operand
        right_operand = self.right_operand.test(item) \
            if isinstance(self.right_operand, Request) else self.right_operand
        return self.operator(item[left_operand], right_operand)

    @classmethod
    def parse_request(cls, request):
        """Convert a ``request`` to a Request object.

        TODO: describe syntaxic sugar.
        
        >>> Site.parse_request({u'a': 1, u'b': None})
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        [Condition(u'a', None, 1),
         Condition(u'b', None, None)]

        """
        if hasattr(request, 'items') and callable(request.items):
            # if it looks like a dict, it is a dict
            return And(*(Condition(key, '=', value) 
                         for key, value in request.items()))
        elif hasattr(request, 'test') and callable(request.test):
            # if it looks like a Request …
            return request
        else:
            # assume a 3-tuple: short for a single condition
            property_name, operator, value = request
            return Condition(property_name, operator, value)


class View_Request(object):
	def _process_aliases(self, aliases):
		my_aliases = {}
		other_aliases = {}
		for key,val in aliases.items():
			if not '.' val:
				my_aliases[key] = val
			else:
				other_aliases[key] = val
		self.aliases = my_aliases
		return other_aliases

	def _process_request(self, request):
		#TODO : remove what we can't manage from the request
		other_requests = {}		
		self.request = request
		return other_requests

	def __init__(self, aliases, request):
		other_aliases = self._process_aliases(aliases)
		other_requests = self._process_request(request)
		self.subviews = self.classify(other_aliases, other_requests)

	def classify(aliases, request):
		
		return subviews


class Condition(Request):
    """Container for ``(property_name, operator, value)``."""
    def __init__(self, property_name, operator, value):
        super(Condition, self).__init__(property_name, operator, value)
    
    @property
    def property_name(self):
        return self.left_operand
    
    @property
    def value(self):
        return self.right_operand
    

class And(Request):
    """Container for ``(first, and, (second, and, (...)))``."""
    def __init__(self, *values):
        values = list(values)
        super(And, self).__init__(values.pop(), OPERATORS["AND"], None)
        if len(values) > 1:
            self.right_operand = And(values)
        else:
            self.right_operand = values.pop()


class Or(Request):
    """Container for ``(first, or, (second, or, (...)))``."""
    def __init__(self, *values):
        values = list(values)
        super(Or, self).__init__(values.pop(), OPERATORS["OR"], None)
        if len(values) > 1:
            self.right_operand = Or(values)
        else:
            self.right_operand = values.pop()

