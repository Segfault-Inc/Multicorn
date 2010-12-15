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
Query
=====

Kalamar query objects.

"""

import itertools
from abc import ABCMeta, abstractmethod
from operator import itemgetter
from kalamar.request import make_request_property
from kalamar.item import AbstractItem

from .request import normalize


class Query(object):
    """Query class."""
    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self, items):
        """Perform the query on ``items``."""
        raise NotImplementedError

    @abstractmethod
    def validate(self, site, properties):
        """Validate the query."""
        raise NotImplementedError


class BadQueryException(Exception):
    """Exception raised when a query cannot be validated."""
    def __init__(self, query, message):
        super(BadQueryException, self).__init__(message)
        self.query = query
        self.message = message


class QueryChain(Query):
    """Chained query.
    
    >>> from kalamar.request import Condition
    >>> items = itertools.cycle([{"a": 1, "b": 1}, {"a": 2, "b": 2}])
    >>> range = QueryRange(slice(1, 4))
    >>> cond = QueryFilter(Condition("a", "=", 2))
    >>> chain = QueryChain([range, cond])
    >>> list(chain(items))
    [{'a': 2, 'b': 2}, {'a': 2, 'b': 2}]
    >>> chain = QueryChain([chain, QueryDistinct()])
    >>> list(chain(items))
    [{'a': 2, 'b': 2}]

    """
    def __init__(self, queries):
        super(QueryChain, self).__init__()
        self.queries = queries

    def __call__(self, items):
        for query in self.queries:
            items = query(items)
        return items

    def validate(self, site, properties):
        for sub_query in self.queries:
            properties = sub_query.validate(site,  properties)
        return properties

    def __repr__(self):
        return "\n-->".join([str(sub) for sub in self.queries])


class QueryDistinct(Query):
    """Query removing duplicate elements.

    >>> items = [{"a": 1, "b": 2}, {"a":2, "b": 3}, {"a": 1, "b": 2}]
    >>> list(QueryDistinct()(items))
    [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}]

    """
    def __call__(self, items):
        return (dict(item_tuple) for item_tuple in
                set(tuple(item.items()) for item in items))

    def validate(self, site, properties):
        return properties

    def __str__(self):
        return "QueryDistinct"


class QueryFilter(Query):
    """Query filtering a set of items.
    
    >>> from kalamar.request import Condition
    >>> cond = Condition("a", "=" , 12)
    >>> items = [{"a": 13, "b": 15}, {"a": 12, "b": 16}]
    >>> filter = QueryFilter(cond)
    >>> list(filter(items))
    [{'a': 12, 'b': 16}]

    """
    def __init__(self, condition):
        super(QueryFilter, self).__init__()
        self.condition = condition

    def __call__(self, items):
        return (item for item in items if self.condition.test(item))

    def validate(self, site, properties):
        try:
            normalize(properties, self.condition)
        except (KeyError, ValueError) as detail:
            raise BadQueryException(self, detail)
        return properties

    def __str__(self):
        return "Filter : %r" % self.condition

    
class QueryOrder(Query):
    """Query ordering a set of items.

    >>> items = [{"a": 4, "b": 8}, {"a": 5 , "b": 7}, {"a": 5, "b": 8}]
    >>> order = QueryOrder([("a", True)])
    >>> order(items)
    [{'a': 4, 'b': 8}, {'a': 5, 'b': 7}, {'a': 5, 'b': 8}]
    >>> order = QueryOrder([("a", True), ("b", False)])
    >>> order(items)
    [{'a': 4, 'b': 8}, {'a': 5, 'b': 8}, {'a': 5, 'b': 7}]

    """
    def __init__(self, orderbys):
        super(QueryOrder, self).__init__()
        self.orderbys = orderbys
    
    def __call__(self, items):
        items = list(items)
        orders = list(self.orderbys)
        orders.reverse()
        for key, order in orders:
            items.sort(key=itemgetter(key), reverse=(not order))
        return items

    def validate(self, site, properties):
        return properties

    def __str__(self):
        return "Order by: %r" % self.orderbys


class QuerySelect(Query):
    """Query selecting a partial view of the items.

    >>> items = [{"a": 2, "b": 3}, {"a":4, "b": 5}]
    >>> select = QuerySelect({"label": "a"})
    >>> list(select(items))
    [{'label': 2}, {'label': 4}]
    >>> list(select([]))
    []

    If ``None`` is given when calling the query, an empty view is
    returned. This behavior is useful for outer joins.

    >>> list(select(None))
    [{'label': None}]

    """
    def __init__(self, mapping=None, object_mapping=None):
        super(QuerySelect, self).__init__()
        if object_mapping is None:
            self.mapping = dict(((name, make_request_property(value))
                                 for name, value in (mapping or {}).items()))
        else:
            self.mapping = object_mapping
        # Classify
        self.sub_selects = {}
        # Dict of dict
        sub_mappings = {}
        for alias, prop in self.mapping.items():
            if prop.child_property is not None:
                sub_mapping = sub_mappings.setdefault(prop.name, {})
                sub_mapping[alias] = prop.child_property
                self.mapping.pop(alias)
        self.sub_selects = dict(((name, QuerySelect(object_mapping = value))
                                 for name, value in sub_mappings.items()))

    def __call__(self, items):
        if isinstance(items, (AbstractItem, dict)):
            items = [items]
        if items is None:
            items = [dict((prop.name, None) for prop in self.mapping.values())]
        for item in items:
            newitem = {}
            for alias, prop in self.mapping.items():
                if prop.name is "*":
                    newitem.update(dict((("%s%s" % (alias, key)), value)
                        for key, value in item.items()))
                else:
                    newitem[alias] = prop.get_value(item)
            if self.sub_selects:
                sub_generators = tuple(
                    sub_select(item[prop] or None) for prop, sub_select
                    in self.sub_selects.items())
                for cartesian_item in itertools.product(*sub_generators):
                    to_yield = dict(newitem)
                    for cartesian_atom in cartesian_item:
                        to_yield.update(cartesian_atom)
                    yield to_yield
            else:
                yield newitem

    def validate(self, site, properties):
        new_props = {}
        for name, prop in self.mapping.items():
            if prop.name is not "*":
                try:
                    old_prop = properties[prop.name]
                except KeyError:
                    raise BadQueryException(self, 
                            "This request has no %r property" % prop.name)
                new_props[name] = old_prop 
            else:
                new_props.update(dict([(("%s%s" % (name, oldname)), oldprop) 
                    for oldname, oldprop in properties.items()]))
        for name, sub_select in self.sub_selects.items():
            try:
                root = properties[name]
                child_properties = root.remote_ap.properties
            except KeyError:
                raise BadQueryException(self, "%r is not a valid property" %
                        name)
            new_props.update(sub_select.validate(site, child_properties))
        return new_props 

    def __str__(self):
        return "SELECT query : %r, %r "  % (self.mapping, self.sub_selects)


class QueryRange(Query):
    """Query selecting a range of items.

    >>> items = itertools.cycle([{"a":1, "b":1},{"a":2, "b": 2}])
    >>> range = QueryRange(slice(1, 3))
    >>> list(range(items))
    [{'a': 2, 'b': 2}, {'a': 1, 'b': 1}]
    
    """
    def __init__(self, query_range):
        super(QueryRange, self).__init__()
        self.range = query_range

    def __call__(self, items):
        return itertools.islice(items, self.range.start, self.range.stop)

    def validate(self, site, properties):
        return properties

    def __str__(self):
        return "Range %r " % self.range
