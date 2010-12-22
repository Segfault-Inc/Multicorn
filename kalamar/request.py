# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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
import re

if "reduce" not in locals():
    from functools import reduce


def re_match(string, pattern):
    """Return if ``string`` matches ``pattern``."""
    return bool(re.search(pattern, string))


def re_not_match(string, pattern):
    """Return if ``string`` does not match ``pattern``."""
    return not re.search(pattern, string)

def like(string, like_pattern):
    """Return if ``string`` does match the 'like' pattern (SQL style)."""
    pattern = re.escape(like_pattern).replace("\%", ".*")
    return re.search(pattern, string)


OPERATORS = {
    "=": eq, "!=": ne, ">": gt, ">=": ge, "<": lt, "<=": le,
    "~=": re_match, "~!=": re_not_match, 'like': like}
REVERSE_OPERATORS = dict((value, key) for key, value in OPERATORS.items())


class OperatorNotAvailable(KeyError):
    """Operator is unknown or not managed."""


def make_request_property(property_name):
    """Return an instance of RequestProperty.

    >>> make_request_property("a")
    a
    >>> make_request_property("a.id")
    a.id

    """
    # property_name is cast to unicode to allow RequestProperty instances
    properties = unicode(property_name).split(".")
    properties.reverse()
    req_prop = RequestProperty(properties[0])
    for prop in properties[1:]:
        req_prop = ComposedRequestProperty(prop, req_prop)
    return req_prop


def make_request(request):
    """Convert a ``request`` to a :class:`Request` object.

    The given argument ``request`` can be:

    - a :class:`Request` object: it is returned unchanged,
    - a ``dict``-like object: an ``And(*(key, =, value))`` request is returned.
    - a ``None``-like object: an all-matching (always true) request is returned,

    """
    if not request:
        return And()
    elif isinstance(request, Request):
        return request
    elif hasattr(request, "items") and hasattr(request.items, "__call__"):
        # If it looks like a dict and smells like a dict, it is a dict.
        return And(
            *(Condition(key, "=", value) for key, value in request.items()))


def normalize(properties, request):
    """Convert the condition values.

    Raises an exception if the property is not supplied or if it can't be cast.

    """
    def _remote_cast(request_property, root_property, value):
        """Recursively cast ``value``."""
        if request_property.child_property:
            return _remote_cast(
                request_property.child_property, root_property.remote_property,
                value)
        else:
            return root_property.cast((value,))[0]

    def _inner_normalize(request):
        """Recursively normalize ``request``."""
        if isinstance(request, (And, Or)):
            requests = [_inner_normalize(sub_request)
                        for sub_request in request.sub_requests]
            return request.__class__(*requests)
        elif isinstance(request, Not):
            return Not(_inner_normalize(request.sub_request))
        elif isinstance(request, Condition):
            root = request.property.name
            if root not in properties:
                raise KeyError(
                    "This access point has no %r property." % root)
            value = _remote_cast(
                request.property, properties[root], request.value)
            return Condition(request.property, request.operator, value)
    return _inner_normalize(make_request(request)).simplify()


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
        return hash(tuple(
                getattr(self, attr) for attr in self._hash_attributes.split()))

    @property
    @abstractmethod
    def properties_tree(self):
        """Tree of properties concerned by this request.

        >>> cond1 = Condition("test.foo", "=", "a")
        >>> cond2 = Condition("test.bar.baz", "=", "b")
        >>> cond3 = Condition("test.bar.spam", "=", "b")
        >>> And(cond1, cond2, cond3).properties_tree # doctest: +ELLIPSIS
        {...'test': {...'foo': foo, ...'bar': {...'baz': baz, ...'spam': spam}}}

        """
        raise NotImplementedError

    def simplify(self):
        """Return a simplified equivalent request."""
        return self


class Condition(Request):
    """Container for ``(property_name, operator, value)``."""
    _hash_attributes = "property operator value"

    def __init__(self, property_name=None, operator="=", value=True):
        super(Condition, self).__init__()
        self.operator_func = OPERATORS[operator]
        self.property = make_request_property(property_name)
        self.operator = operator
        self.value = value

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__, self.property, self.operator, self.value)

    def test(self, item):
        """Return if ``item`` matches the request."""
        return self.operator_func(self.property.get_value(item), self.value)

    @property
    def properties_tree(self):
        def inner_properties(prop):
            """Recursively get children properties of ``prop``."""
            if prop.child_property:
                return {prop.name: inner_properties(prop.child_property)}
            else:
                return {prop.name: prop}
        return inner_properties(self.property)


class RequestProperty(object):
    """Property from an item.

    This object should be used to retrieve a property value from an item.

    """
    def __init__(self, name):
        self.name = name
        self.child_property = None

    def get_value(self, item):
        """Retrieve the value from this property from an item."""
        return item[self.name] if item else None

    def __repr__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.__hash__() == other.__hash__()


class ComposedRequestProperty(RequestProperty):
    """Nested property from an item.

    A nested property is of the form ``foo.bar.baz``.

    """
    def __init__(self, name, child_property, inner=True):
        super(ComposedRequestProperty, self).__init__(name)
        self.name = name
        self.child_property = child_property
        self.inner = inner

    def get_value(self, item):
        return self.child_property.get_value(item[self.name])

    def __repr__(self):
        return "%s.%r" % (self.name, self.child_property)

    def __hash__(self):
        return hash((hash(self.name), hash(self.child_property)))


class _AndOr(Request):
    """Super class for And and Or that holds identical behavior."""
    __metaclass__ = ABCMeta
    _hash_attributes = "sub_requests __class__"

    def __init__(self, *sub_requests):
        super(_AndOr, self).__init__()
        # A frozenset in unordered: And(a, b) == And(b, a)
        # and its elements are unique: And(a, a) = And(a)
        self.sub_requests = frozenset(sub_requests)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(repr(request) for request in self.sub_requests))

    @abstractmethod
    def test(self, item):
        raise NotImplementedError

    @property
    def properties_tree(self):
        def merge_properties(tree1, tree2):
            """Merge two properties trees into one."""
            for name, tree1_values in tree1.items():
                tree2_values = tree2.setdefault(name, {})
                if isinstance(tree2_values, dict):
                    tree2[name] = merge_properties(tree2_values, tree1_values)
            return tree2

        trees = [sub.properties_tree for sub in self.sub_requests] or [{}]
        return reduce(merge_properties, trees)

    def flatten(self):
        """Return a generator of flat sub requests."""
        for sub_request in self.sub_requests:
            if type(sub_request) is type(self):
                for sub_sub in sub_request.flatten():
                    yield sub_sub
            else:
                yield sub_request

    def simplify(self):
        new_sub_requests = set(request.simplify() for request in self.flatten())
        if len(new_sub_requests) == 1:
            # And(a) == a
            return new_sub_requests.pop()
        return type(self)(*new_sub_requests)


class And(_AndOr):
    """True if all given requests are true."""
    def test(self, item):
        return all(request.test(item) for request in self.sub_requests)


class Or(_AndOr):
    """True if at least one given requests is true."""
    def test(self, item):
        return any(request.test(item) for request in self.sub_requests)


class Not(Request):
    """Negate a request."""
    _hash_attributes = "sub_request __class__"

    def __init__(self, sub_request):
        super(Not, self).__init__()
        self.sub_request = sub_request

    def __repr__(self):
        return "Not(%r)" % self.sub_request

    def test(self, item):
        return not self.sub_request.test(item)

    @property
    def properties_tree(self):
        return self.sub_request.properties_tree

    def simplify(self):
        if isinstance(self.sub_request, Not):
            # Not(Not(a)) == a
            return self.sub_request.sub_request.simplify()
        else:
            return Not(self.sub_request.simplify())
