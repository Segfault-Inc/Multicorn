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

    def walk(self, func, values=[]):
        """ Returns a list containing the result from applying func to each child """
        return func(self)

    def test(self, item):
        """Return if :prop:`item` matches the request."""
        left_operand = self.left_operand.test(item) \
            if isinstance(self.left_operand, Request) else self.left_operand
        right_operand = self.right_operand.test(item) \
            if isinstance(self.right_operand, Request) else self.right_operand
        return self.operator(item[left_operand], right_operand)



class RequestMetaData(object):

    def __init__(request):
        self.request = request

    def concerned_properties:


class View_Request(object):

    def __init__(self,access_point, aliases, request):
		self.other_aliases = {}
        self.aliases = {}
        self._process_aliases(aliases)
		other_requests = self._process_request(request)
		self.subviews = self.classify(other_aliases, other_requests)
        self.access_point = access_point

	def _process_aliases(self, aliases):
		my_aliases = {}
		for key,val in aliases.items():
			if not '.' val:
				self.aliases[key] = val
			else:
				self.other_aliases[key] = val

    def _extract_foreign_condition(self, request):
        foreign_conditions = []
        self_conditions = []
        if request.operator == OPERATORS["AND"]:
            for op in (request.left_operand, request.right_operand):
                self_requests, foreign_requests = self._extract_foreign_condition(request.op)
                self_conditions.extend(self_requests)
                foreign_conditions.extend(foreign_requests)
        elif request.operator == OPERATORS["OR"]:
            for op in (request.left_operand, request.right_operand):
                self_re


        return foreign_conditions

        
         

	def classify(self):
        """ Build subviews from the aliases and request """
        self.subviews = {}
        self.joins = {}
        for alias, property_path in self.other_aliases.items():
            splitted_path = property_path.split(".")
            root = splitted_path[0]
            is_outer_join = root.startswith("<")
            if is_outer_join:
                root = root[1:]
            if root not in self.subviews:
                access_point = self.access_point.properties[root].access_point
                subview = View_Request(access_point, {}, None)
            else : 
                subview = self.subviews[root]
            subview.aliases[alias] = splitted_path[1:].join(".")
            self.joins[root] = is_outer_join 
            
		return subviews
		
		 

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
