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
            self.operator = OPERATORS[operator]
        except KeyError:
            raise OperatorNotAvailable('Operator %r is not supported here.'
                                       % operator)
        self.property_name = property_name
        self.value = value

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__,
            self.property_name,
            REVERSE_OPERATORS[self.operator],
            self.value)

    def test(self, item):
        """Return if :prop:`item` matches the request."""
        return self.operator(item[self.property_name], self.value)
    
    def walk(self, func, values=None):
        """ Returns a dict containing the result from applying func to each child """
        values = values or {}
        values[self] = func(self)
        return values


class And(Request):
    """True if all given requests are true."""
    def __init__(self, *requests):
        self.requests = requests
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, 
                           ', '.join(map(repr, self.requests)))

    def test(self, item):
        return all(request.test(item) for request in self.requests)


class Or(Request):
    """True if at least one given requests are true."""
    def __init__(self, *requests):
        self.requests = requests
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, 
                           ', '.join(map(repr, self.requests)))

    def test(self, item):
        return any(request.test(item) for request in self.requests)


class Not(Request):
    """Negates a request."""
    def __init__(self, request):
        self.request = request
    
    def test(self, item):
        return not self.request.test(item)


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
        self._my_requests = []
        self.aliases = aliases
        self.my_aliases = {}
        self._other_aliases = {}
        self.request = request
        self.subviews = {}
        self.joins = {}
        self.orphan_request = []
        #Process the bouzin
        self._process_aliases(aliases)
        self.classify()

    def _process_aliases(self, aliases):
        for key,val in aliases.items():
            if '.' not in val:
                self.my_aliases[key] = val
            else:
                self._other_aliases[key] = val

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

    def real_prop_name(self, prop_name):
        return prop_name[1:] if prop_name.startswith("<") else prop_name

    def root(self, prop_name):
        splitted = prop_name.split(".")
        rest = ".".join(splitted[1:]) if len(splitted) > 1 else None
        return self.real_prop_name(splitted[0]),rest

    def _classify_request(self, request, conds_by_prop=None):
        conds_by_prop = conds_by_prop or {}
        if isinstance(request, Condition):
            root, rest = self.root(request.property_name)
            if not rest : 
                self._my_requests.append(request)
            else: 
                newcond = Condition(rest, request.operator, request.value)
                self.joins[root] = True
                conds_for_prop = conds_by_prop.get(root,And())
                conds_for_prop.requests.append(newcond)
                conds_by_prop[root] = conds_for_prop
        elif isinstance(request, Or):
            #TODO: manage Or which belong strictly to the property, and 
            # should therefore be kept
            self.orphan_request.append(request)
            return {}
        elif isinstance(request, And):
            for branch in request.requests:
                conds_by_prop.update(self._classify_request(branch, conds_by_prop))
        elif isinstance(request, Not):
            conds_by_prop.update(self._classify_request(request.request, conds_by_prop))
        return conds_by_prop
        



    def classify(self):
        """Build subviews from the aliases and request."""
        self.subviews = {}
        self.joins = {}
        conditions = {}
        aliases = self._classify_alias()
        conditions = self._classify_request(self.request)
        self.request = And(self._my_requests)
        #genereates the subviews from the processed aliases and requests
        for key in self.joins:
            access_point = self.access_point.properties[key].remote_ap
            req = conditions.get(key, Request.parse({}))
            subview = ViewRequest(access_point, aliases.get(key,{}), req)
            self.subviews[key] = subview






