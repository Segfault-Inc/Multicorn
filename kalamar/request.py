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

OPERATORS = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
#    "~=": re_match,
#    "~!=": re_not_match
}


class OperatorNotAvailable(ValueError):
    pass

class Request(object):
    """Abstract class for kalamar requests."""
    def test(self, item):
        """Return if :prop:`item` matches the request."""
        raise NotImplementedError('Abstract class.')

    def walk(self, func, values=[]):
        """ Returns a list containing the result from applying func to each child """
        return func(self)

    @classmethod
    def parse(cls, request):
        """Convert a ``request`` to a Request object.

        TODO: describe syntaxic sugar.
        
        >>> Request.parse({u'a': 1, u'b': 'foo'})
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        And(Condition(u'b', '=', 'foo'), <built-in function and_>, 
            Condition(u'a', '=', 1))
        """
        if not request:
            # empty request
            return And()
        elif hasattr(request, 'items') and callable(request.items):
            # If it looks like a dict and smell like a dict, it is a dict.
            return And(*(Condition(key, '=', value) 
                         for key, value in request.items()))
        elif hasattr(request, 'test') and callable(request.test):
            # If it looks like a Request …
            return request
        else:
            # Assume a 3-tuple: short for a single condition
            property_name, operator, value = request
            return Condition(property_name, operator, value)


class Condition(Request):
    """Container for ``(property_name, operator, value)``."""
    def __init__(self, property_name, operator, value):
        self.prop_name = prop_name
        self.operator = operator
        self.value = value

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__, self.prop_name, self.operator,
            self.value)

    def test(self, item):
        """Return if :prop:`item` matches the request."""
        return self.operator(item[self.property_name], self.value)

    def walk(self, func, values=[]):
        """ Returns a list containing the result from applying func to each child """
        for branch in (self.left_operand, self.right_operand):
            values.extend(branch.walk(func,values))
        return values

#class CompositeRequest(Request):
#    """Abstract class for composite requests, such as "AND" and "OR" 
#    
#    Both operands should be Requests
#    
#    """ 
#    def walk(self, func, values=[]):
#        """ Returns a list containing the result from applying func to each leaf node """
#        for branch in (self.left_operand, self.right_operand):
#            values.extend(branch.walk(func,values))
#        return values


class And(Request):
    """True if all given requests are true."""
    def __init__(self, *requests):
        self.requests = requests
    
    def test(self, item):
        return all(request.test(item) for request in self.requests)

class Or(Request):
    """True if at least one given requests are true."""
    def __init__(self, *requests):
        self.requests = requests
    
    def test(self, item):
        return any(request.test(item) for request in self.requests)

class Not(Request):
    """Negates a request."""
    def __init__(self, request):
        self.request = request
    
    def test(self, item):
        return not self.request.test(item)


class View_Request(object):
    """ Class storing the information needed for a view 
    
        The following attributes are available : 
            - aliases : all aliases as defined when calling view
            - my_aliases : aliases concerning the access_point directly
                (i.e., the path consists of only a property from the ap)
            - joins: a dict mapping property name to a boolean indicating 
                wether the join should be outer or not (True: outer join, False: inner join)
            _ subviews : a dict mapping property_names to View_Request objects
                
        
    
    """

    
    def __init__(self,access_point, aliases, request):
        self.aliases = aliases
        self.my_aliases = {}
        self._other_aliases = {}
        self.request = request
        self.subviews = {}
        self.joins = {}
        self._process_aliases(aliases)
        self.classify()

	def _process_aliases(self, aliases):
		for key,val in aliases.items():
			if '.' not in val:
				self.my_aliases[key] = val
			else:
				self._other_aliases[key] = val

    def classify(self):
        """ Build subviews from the aliases and request """
        self.subviews = {}
        self.joins = {}
        for alias, property_path in self._other_aliases.items():
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
        
        def join_from_request(request):
            root = request.left_operand.split(".")[0]
            root = root[1:] if root.startswith("<") else root
            return root,request

        #Builds a dict mapping property_names to elementary Condition 
        joins_from_request = sorted(self.request.walk(join_from_request),lambda x,y : x)
        joins_from_request = dict([(key, list(group)) 
            for key,group in groupby(joins_from_request, lambda x,y: x)])
        for key, value in joins_from_request.items():
            self.joins[key] = True

        self.joins.update(dict([(key,True) for key in joins_from_request]))



