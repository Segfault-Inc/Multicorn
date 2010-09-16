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
from itertools import groupby

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
REVERSE_OPERATORS = dict((value, key) for key, value in OPERATORS.items())

class OperatorNotAvailable(KeyError):
    pass


class Request(object):
    """Abstract class for kalamar requests."""
    def test(self, item):
        """Return if :prop:`item` matches the request."""
        raise NotImplementedError('Abstract class.')

    def walk(self, func, values=None):
        """ Returns a dict containing the result from applying func to each child """
        values = values or {}
        for branch in (self.requests):
            values = branch.walk(func,values)
        return values

    @classmethod
    def parse(cls, request):
        """Convert a ``request`` to a Request object.

        TODO: describe syntaxic sugar.
        
        XXX This doctest relies on the order of a dict. TODO: Fix this.
        >>> Request.parse({u'a': 1, u'b': 'foo'})
        ...                                  # doctest: +NORMALIZE_WHITESPACE
        And(Condition(u'a', '=', 1), Condition(u'b', '=', 'foo'))
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
        try:
            self.operator_func = OPERATORS[operator]
        except KeyError:
            raise OperatorNotAvailable('Operator %r is not supported here.'
                                       % operator)
        self.property_name = property_name
        self.operator = operator
        self.value = value

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__,
            self.property_name,
            self.operator,
            self.value)

    def test(self, item):
        """Return if :prop:`item` matches the request."""
        return self.operator_func(item[self.property_name], self.value)
    
    def walk(self, func, values=None):
        """ Returns a dict containing the result from applying func to each child """
        values = values or {}
        values[self] = func(self)
        return values


class And(Request):
    """True if all given requests are true."""
    def __init__(self, *sub_requests):
        self.sub_requests = sub_requests
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, 
                           ', '.join(map(repr, self.sub_requests)))

    def test(self, item):
        return all(request.test(item) for request in self.sub_requests)


class Or(Request):
    """True if at least one given requests are true."""
    def __init__(self, *sub_requests):
        self.sub_requests = sub_requests
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, 
                           ', '.join(map(repr, self.sub_requests)))

    def test(self, item):
        return any(request.test(item) for request in self.sub_requests)


class Not(Request):
    """Negates a request."""
    def __init__(self, sub_request):
        self.sub_request = sub_request
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.sub_request)

    def test(self, item):
        return not self.sub_request.test(item)


class ViewRequest(object):
    """Class storing the information needed for a view.
    
    :attribute aliases: all aliases as defined when calling view
    :attribute my_aliases: aliases concerning the access_point directly
      (i.e., the path consists of only a property from the ap)
    :attribute joins: a dict mapping property name to a boolean indicating 
      wether the join should be outer or not (True: outer, False: inner)
    :attribute subviews: a dict mapping property_names to View_Request objects
    
    """
    def __init__(self,access_point, aliases, request):
        #Initialize instance attributes
        self.request = request
        self.access_point = access_point
        self.aliases = aliases
        self.my_aliases = {}
        self._other_aliases = {}
        self.request = request
        self.subviews = {}
        self.joins = {}
        self.orphan_request = {}
        #Process the bouzin
        self._process_aliases(aliases)
        self.classify()

    def _process_aliases(self, aliases):
        for key,val in aliases.items():
            if '.' not in val:
                self.my_aliases[key] = val
            else:
                self._other_aliases[key] = val

    def _classify_request(self, request):
        def transform_node(node):
            if node.operator == OPERATORS['OR']:
                return node
            else:
                return None
        
    def _classify(self):
        """ Build subviews from the aliases and request """
        for key, value in aliases.items():
            if "." not in val:
                self.my_aliases[key] = value
            else:
                self._other_aliases[key] = value

    def _classify_alias(self):
        """ Returns a dict mapping properties from this access point to 
        to the alias it should manage
        """
        aliases = {}
        for alias, property_path in self._other_aliases.items():
            splitted_path = property_path.split(".")
            root = splitted_path[0]
            is_outer_join = root.startswith("<")
            if is_outer_join:
                root = root[1:]
            subaliases = aliases.get(root,{})
            subaliases.update({alias: ".".join(splitted_path[1:])})
            aliases[root] = subaliases
            self.joins[root] = is_outer_join 
        return aliases

    def _classify_request(self):
        """Builds a dict mapping property names to the condition they manage
        """
        def join_from_request(request):
            path = request.property_name.split(".")
            if len(path) == 1 :
                return None
            else:
                root = path[0]
                root = root[1:] if root.startswith("<") else root
                return root
        joins_from_request = sorted(self.request.walk(join_from_request).items())
        joins_from_request = dict([(key, list(group)) 
            for key,group in groupby(joins_from_request, lambda x: x[1]) if key])
        for key, value in joins_from_request.items():
            self.joins[key] = True
        self.joins.update(dict([(key,True) for key in joins_from_request]))
        return joins_from_request 



    def classify(self):
        """Build subviews from the aliases and request."""
        self.subviews = {}
        self.joins = {}
        conditions = {}
        aliases = self._classify_alias()
        joins_from_request = self._classify_request()
        #genereates the subviews from the processed aliases and requests
        for key in self.joins:
            access_point = self.access_point.properties[key].remote_ap
            req = conditions.get(key, Request.parse({}))
            subview = ViewRequest(access_point, aliases.get(key,{}), req)
            self.subviews[key] = subview





