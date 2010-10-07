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

from operator import eq, ne, gt, ge, lt, le
from abc import ABCMeta, abstractmethod


OPERATORS = {"=": eq, "!=": ne, ">": gt, ">=": ge, "<": lt, "<=": le}
REVERSE_OPERATORS = dict((value, key) for key, value in OPERATORS.items())


class OperatorNotAvailable(KeyError):
    """Operator is unknown or not managed."""


def _flatten(request):
    """Take And or Or ``request``, return a generator of flat sub requests."""
    for sub_request in request.sub_requests:
        if sub_request.__class__ is request.__class__:
            for sub_sub in _flatten(sub_request):
                yield sub_sub
        else:
            yield sub_request

def simplify(request):
    """Return a simplified equivalent request."""
    if isinstance(request, (And, Or)):
        # _flatten: And(a, And(b, c))) == And(a, b, c)
        # Use a set to remove duplicates: And(a, b, b) == And(a, b)
        new_sub_requests = set(simplify(r) for r in _flatten(request))
        if len(new_sub_requests) == 1:
            # And(a) == a
            return new_sub_requests.pop()
        return request.__class__(*new_sub_requests)
    elif isinstance(request, Not):
        if isinstance(request.sub_request, Not):
            # Not(Not(a)) == a
            return simplify(request.sub_request.sub_request)
        else:
            return Not(simplify(request.sub_request))
    else:
        return request

def normalize(properties, request):
    """Convert a ``request`` to a Request object.

    TODO: describe syntaxic sugar.

    """
    def _inner_normalize(request):
        if not request:
            # Empty request: always true.
            return And()
        elif hasattr(request, "items") and callable(request.items):
            # If it looks like a dict and smells like a dict, it is a dict.
            return And(*(_inner_normalize(Condition(key, "=", value))
                         for key, value in request.items()))
        elif isinstance(request, (And, Or)):
            return request.__class__(*(_inner_normalize(r) 
                for r in request.sub_requests))
        elif isinstance(request, Not):
            return Not(_inner_normalize(request.sub_request))
        elif isinstance(request, Condition):
            # TODO: decide where the Condition.root method should be
            root = request.property.property_name
            if root not in properties:
                raise KeyError(
                    "This access point has no %r property." % root)
            # TODO: validate sub requests 
            value = properties[root].cast((request.value,))[0]
            return Condition(property = request.property,
                    operator=request.operator, value=value)
        else:
            # Assume a 3-tuple: short for a single cond
            return _inner_normalize(Condition(*request))
    return simplify(_inner_normalize(request))


class Request(object):
    """Abstract class for kalamar requests."""
    __metaclass__ = ABCMeta

    _hash_attributes = ""
    
    @abstractmethod
    def test(self, item):
        """Return if ``item`` matches the request."""
        raise NotImplementedError

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.__hash__() == other.__hash__()

    def __hash__(self):
        return hash(self.__hash_tuple())

    def __hash_tuple(self):
        return tuple(
            getattr(self, attr) for attr in self._hash_attributes.split())


class Condition(Request):
    """Container for ``(property_name, operator, value)``."""

    _hash_attributes = "property operator value"

    def __init__(self, property_name = None, operator='=', value=True, property=None):
        try:
            self.operator_func = OPERATORS[operator]
        except KeyError:
            raise OperatorNotAvailable(
                "Operator %r is not supported here." % operator)
        if property is not None:
            self.property = property
        else:
            self.property = self.__build_property(property_name)
        self.operator = operator
        self.value = value

    def __build_property(self, property_name):
        properties = property_name.split(".")
        leaf = RequestProperty(properties[-1])
        req_prop = leaf
        for prop in properties[-1:0]:
            req_prop = ComposedRequestProperty(prop, req_prop)
        return req_prop


    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__,
            self.property,
            self.operator,
            self.value)
    
    def test(self, item):
        """Return if ``item`` matches the request."""
        return self.operator_func(self.property.getValue(item), self.value)

class RequestProperty(object):

    def __init__(self, property_name):
        self.property_name = property_name

    def kalamarProperty(self, access_point):
        return access_point.properties[self.property_name]

    def getValue(self, item):
        return item[self.property_name]

    def __repr__(self):
        return self.property_name


    def __hash__(self):
        return hash(self.property_name)


class ComposedRequestProperty(RequestProperty):

    def __init__(self, property_name, child_property, inner = True):
        self.inner = inner
        self.property_name = property_name
        self.child_property = child_property

    def kalamarProperty(self, access_point):
        root = super(RequestProperty, self).kalamarProperty(access_point)
        remote_ap = access_point.site.access_points[root.remote_ap]
        return self.child_property.kalamarProperty(remote_ap)

    def getValue(self, item):
        return child_property.getValue(item[property_name])

    def __repr__(self):
        return "%s.%r" % (self.property_name, self.child_property)

    def __hash__(self):
        return hash(tuple(hash(self.property_name, hash(self.child_property))))

    

class _AndOr(Request):
    """Super class for And and Or that holds identical behavior."""
    __metaclass__ = ABCMeta

    _hash_attributes = "sub_requests __class__"

    def __init__(self, *sub_requests):
        # A frozenset in unordered: And(a, b) == And(b, a)
        # and it’s elements are unique : And(a, a) = And(a)
        self.sub_requests = frozenset(sub_requests)
    
    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(repr(request) for request in self.sub_requests))


class And(_AndOr):
    """True if all given requests are true."""
    def test(self, item):
        return all(request.test(item) for request in self.sub_requests)


class Or(_AndOr):
    """True if at least one given requests are true."""
    def test(self, item):
        return any(request.test(item) for request in self.sub_requests)


class Not(Request):
    """Negate a request."""
    _hash_attributes = "sub_request __class__"

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
    def __init__(self, aliases, request):
        # Initialize instance attributes
        self.aliases = dict(aliases)
        self._subaliases = {}
        self._request = And()
        self._original_request = request
        self.additional_aliases = {}
        self.additional_request = And()
        self.subviews = {}
        self.joins = {}
        self.orphan_request = And()
        # Process
        self.classify(request)

    @property
    def request(self):
        return simplify(And(self._request, self.additional_request))

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
                subaliases = self._subaliases.get(root, {})
                subaliases.update({alias: ".".join(splitted_path[1:])})
                self._subaliases[root] = subaliases
                self.joins[root] = is_outer_join 
                self.aliases.pop(alias)
        return aliases

    def _build_orphan(self, request):
        if isinstance(request, Or):
            return Or(*(self._build_orphan(subreq)
                        for subreq in request.sub_requests))
        elif isinstance(request, And):
            return And(*(self._build_orphan(subreq)
                         for subreq in request.sub_requests))
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
            if not rest:
                self._request = And(self._request, request)
            else: 
                newcond = Condition(rest, request.operator, request.value)
                self.joins[root] = True
                conds_for_prop = conds_by_prop.get(root, [])
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
                self._subaliases.get(key, {}), And(*req))
            self.subviews[key] = subview
