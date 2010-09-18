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
Request
=======

Kalamar request objects and operator helpers.

"""

import operator
from abc import ABCMeta, abstractmethod
from itertools import groupby

from .value import PROPERTY_TYPES, to_type
from .property import Property

OPERATORS = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le}
REVERSE_OPERATORS = dict((value, key) for key, value in OPERATORS.items())


class OperatorNotAvailable(KeyError):
    """Operator is unknown or not managed."""
    

def normalize_request(properties, request):
    """Convert a ``request`` to a Request object.

    TODO: describe syntaxic sugar.
    TODO: more unit tests here.
    XXX This doctests rely on the order of a dict. TODO: Fix this.
    
    >>> properties = {'a': Property(int), 'b': Property(str)}
    >>> normalize_request(properties, {u'a': 1, u'b': 'foo'})
    And(Condition(u'a', '=', 1), Condition(u'b', '=', 'foo'))

    >>> properties = {'a': Property(float), 'b': Property(unicode)}
    >>> normalize_request(properties, {u'a': 1, u'b': 'foo'})
    And(Condition(u'a', '=', 1.0), Condition(u'b', '=', u'foo'))

    >>> properties = {'a': Property(float), 'b': Property(int)}
    >>> normalize_request(properties, {u'a': 1, u'b': 'foo'})
    ... # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: ...

    """
    if not request:
        # Empty request
        return And()
    elif hasattr(request, "items") and callable(request.items):
        # If it looks like a dict and smells like a dict, it is a dict.
        return And(*(normalize_request(properties, Condition(key, "=", value))
                     for key, value in request.items()))
    elif isinstance(request, And):
        return And(*(normalize_request(properties, r)
                     for r in request.sub_requests))
    elif isinstance(request, Or):
        return Or(*(normalize_request(properties, r)
                    for r in request.sub_requests))
    elif isinstance(request, Not):
        return Not(normalize_request(properties, request.sub_request))
    elif isinstance(request, Condition):
        # TODO decide where the Condition.root method should be
        root, rest = request.root()
        if root not in properties:
            raise KeyError(
                "This access point has no %r property." % root)
        property_type = properties[root].type
        # TODO : validate sub requests 
        if rest :
            return request
        elif property_type in PROPERTY_TYPES:
            value = PROPERTY_TYPES[property_type](request.value)
        else:
            value = to_type(request.value, property_type)
        return Condition(request.property_name, request.operator, value)
    else:
        # Assume a 3-tuple: short for a single condition
        property_name, operator, value = request
        return normalize_request(properties, 
            Condition(property_name, operator, value))


class Request(object):
    """Abstract class for kalamar requests."""
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def test(self, item):
        """Return if :prop:`item` matches the request."""
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other):
        raise NotImplementedError
    
    @abstractmethod
    def walk(self, func, values=None):
        """Returns a dict of the result from applying ``func`` to each child."""
        raise NotImplementedError

    @classmethod
    def parse(cls, access_point, request):
        self.parse(request)


class Condition(Request):
    """Container for ``(property_name, operator, value)``."""
    def __init__(self, property_name, operator, value):
        try:
            self.operator_func = OPERATORS[operator]
        except KeyError:
            raise OperatorNotAvailable(
                "Operator %r is not supported here." % operator)
        self.property_name = property_name
        self.operator = operator
        self.value = value
    
    def root(self):
        splitted = self.property_name.split(".")
        rest = ".".join(splitted[1:]) if len(splitted) > 1 else None
        return splitted[0], rest

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__,
            self.property_name,
            self.operator,
            self.value)
    
    def __eq__(self, other):
        return isinstance(other, Condition) and \
            (self.property_name, self.operator, self.value) == \
            (other.property_name, other.operator, other.value)

    def test(self, item):
        """Return if ``item`` matches the request."""
        return self.operator_func(item[self.property_name], self.value)
    
    def walk(self, func, values=None):
        """Returns a dict of the result from applying ``func`` to each child."""
        values = values or {}
        values[self] = func(self)
        return values


class _And_or_Or(Request):
    """Super class for And and Or that holds identical behavior."""
    def __init__(self, *sub_requests):
        self.sub_requests = []
        for sub_req in sub_requests:
            # Both And and Or are associative.
            if isinstance(sub_req, self.__class__):
                self.sub_requests.extend(sub_req.sub_requests)
            else :
                self.sub_requests.append(sub_req)
    
    def __eq__(self, other):
        return isinstance(other, self.__class__) \
            and self.sub_requests == other.sub_requests

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(map(repr, self.sub_requests)))

    def walk(self, func, values=None):
        values = values or {}
        for branch in self.sub_requests:
            values = branch.walk(func,values)
        return values


class And(_And_or_Or):
    """True if all given requests are true."""
    def test(self, item):
        return all(request.test(item) for request in self.sub_requests)


class Or(_And_or_Or):
    """True if at least one given requests are true."""
    def test(self, item):
        return any(request.test(item) for request in self.sub_requests)


class Not(Request):
    """Negates a request."""
    def __init__(self, sub_request):
        self.sub_request = sub_request
    
    def __eq__(self, other):
        return isinstance(other, Not) and self.sub_request == other.sub_request

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.sub_request)

    def test(self, item):
        return not self.sub_request.test(item)

    def walk(self, func, values=None):
        return self.sub_request.walk(values or {})


class ViewRequest(object):
    """Class storing the information needed for a view.
    
    :attribute aliases: all aliases as defined when calling view
    :attribute my_aliases: aliases concerning the access_point directly
      (i.e., the path consists of only a property from the ap)
    :attribute joins: a dict mapping property name to a boolean indicating 
      wether the join should be outer or not (True: outer, False: inner)
    :attribute subviews: a dict mapping property_names to View_Request objects
    
    """
    def __init__(self, aliases, request):
        #Initialize instance attributes
        self.aliases = dict(aliases)
        self._subaliases = {}
        self._request = And()
        self._original_request = request
        self.additional_aliases = {}
        self.additional_request = And()
        self.subviews = {}
        self.joins = {}
        self.orphan_request = And()
        #Process the bouzin
        self.classify(request)

    @property
    def request(self):
        return And(self._request, self.additional_request)

    def _classify_alias(self):
        """Classify request aliases.

        Return a dict mapping properties from this access point to to the alias
        it should manage.

        """
        aliases = {}
        for alias, property_path in self.aliases.items():
            splitted_path = property_path.split(".")
            if len(splitted_path) > 1:
                root = splitted_path[0]
                is_outer_join = root.startswith("<")
                if is_outer_join:
                    root = root[1:]
                subaliases = self._subaliases.get(root,{})
                subaliases.update({alias: ".".join(splitted_path[1:])})
                self._subaliases[root] = subaliases
                self.joins[root] = is_outer_join 
                self.aliases.pop(alias)
        return aliases

    def _build_orphan(self, request):
        if isinstance(request, Or):
            return apply(Or, [self._build_orphan(subreq)
                              for subreq in request.sub_requests])
        elif isinstance(request, And):
            return apply(And, [self._build_orphan(subreq)
                               for subreq in request.sub_requests])
        elif isinstance(request, Condition):
            root, rest = request.root()
            alias = "_____" + request.property_name
            remote_aliases = self._subaliases.get(root, {})
            remote_aliases[alias] = rest
            self._subaliases[root] = remote_aliases
            self.additional_aliases[alias] = request.property_name
            cond = Condition(alias, request.operator, request.value)
            return cond

    def _classify_request(self, request, conds_by_prop=None):
        conds_by_prop = conds_by_prop or {}
        if isinstance(request, Condition):
            root, rest = request.root()
            if not rest : 
                self._request = And(self._request, request)
            else: 
                newcond = Condition(rest, request.operator, request.value)
                self.joins[root] = True
                conds_for_prop = conds_by_prop.get(root,[])
                conds_for_prop.append(newcond)
                conds_by_prop[root] = conds_for_prop
        elif isinstance(request, Or):
            # TODO: manage Or which belong strictly to the property, and 
            # should therefore be kept
            self.orphan_request = And(
                self.orphan_request,self._build_orphan(request))
            return {}
        elif isinstance(request, And):
            for branch in request.sub_requests:
                conds_by_prop.update(
                    self._classify_request(branch, conds_by_prop))
        elif isinstance(request, Not):
            conds_by_prop.update(
                self._classify_request(request.subrequest, conds_by_prop))
        return conds_by_prop

    def classify(self, request):
        """Build subviews from the aliases and request."""
        self.subviews = {}
        self.joins = {}
        conditions = {}
        self._classify_alias()
        conditions = self._classify_request(request)
        # Genereates the subviews from the processed aliases and requests
        for key in self.joins:
            req = conditions.get(key, [])
            subview = ViewRequest(
                self._subaliases.get(key, {}), apply(And, req))
            self.subviews[key] = subview
