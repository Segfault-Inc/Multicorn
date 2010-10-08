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
Query
=====

Kalamar query objects.

"""

import itertools
from abc import ABCMeta, abstractmethod
from operator import itemgetter
from kalamar.request import make_request_property
from kalamar.item import Item

from .request import normalize


class Query(object):
    """Query class.

    TODO: describe this (useless?) class

    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self, items):
        raise NotImplementedError

    @abstractmethod
    def validate(self, site, query, properties):
        raise NotImplementedError


class QueryChain(Query):
    """Chained query.
    
    >>> from kalamar.request import Condition
    >>> items = itertools.cycle([{'a':1, 'b':1},{'a':2, 'b': 2}])
    >>> range = QueryRange(slice(1,4))
    >>> cond = QueryFilter(Condition("a", "=", 2))
    >>> chain = QueryChain([range, cond])
    >>> list(chain(items))
    [{'a': 2, 'b': 2}, {'a': 2, 'b': 2}]
    >>> chain = QueryChain([chain, QueryDistinct()])
    >>> list(chain(items))
    [{'a': 2, 'b': 2}]

    """
    def __init__(self, queries):
        self.queries = queries

    def __call__(self, items):
        for query in self.queries:
            items = query(items)
        return items

    def validate(self, site, query, properties):
        for sub_query in query.queries:
            valid, properties = sub_query.validate(site, sub_query, properties)
            if not valid:
                return False, properties
        return True, properties


class QueryDistinct(Query):
    """Query removing duplicate elements.

    >>> items = [{'a': 1, 'b': 2}, {'a':2, 'b': 3}, {'a': 1, 'b': 2}]
    >>> list(QueryDistinct()(items))
    [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}]

    """
    def __call__(self, items):
        return (dict(item_tuple) for item_tuple in
                set(tuple(item.items()) for item in items))

    def validate(self, site, query, properties):
        return True, properties


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
        self.condition = condition

    def __call__(self, items):
        return (item for item in items if self.condition.test(item))

    def validate(self, site, query, properties):
        return normalize(properties, query.condition), properties


    
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
        self.orderbys = orderbys
    
    def __call__(self, items):
        items = list(items)
        orders = list(self.orderbys)
        orders.reverse()
        for key, order in orders:
            items.sort(key=itemgetter(key), reverse=(not order))
        return items

    def validate(self, site, query, properties):
        for orderby in self.orderbys:
            if not hasattr(orderby, "__hash__"):
                return False, properties
        return True, properties


class QuerySelect(Query):
    """Query selecting a partial view of the items.

    >>> items = [{"a": 2, "b": 3}, {"a":4, "b": 5}]
    >>> select = QuerySelect({'label': 'a'})
    >>> list(select(items))
    [{'label': 2}, {'label': 4}]

    """
    def __init__(self, mapping=None, object_mapping=None):
        if object_mapping is None:
            self.mapping = dict(((name, make_request_property(value))
                                 for name, value in (mapping or {}).items()))
        else:
            self.mapping = object_mapping
        self.__mapping = dict(self.mapping)

        # Classify
        self.sub_selects = {}
        # Dict of dict
        sub_mappings = {}
        for alias, prop in self.mapping.items():
            if prop.child_property is not None:
                sub_mapping = sub_mappings.setdefault(prop.name, {})
                sub_mapping[alias] = prop.child_property
                self.__mapping.pop(alias)
        self.sub_selects = dict(((name, QuerySelect(object_mapping = value))
                                 for name, value in sub_mappings.items()))

    def __call__(self, items):
        if isinstance(items, Item) or isinstance(items, dict):
            items = (items,)
        for item in items:
            newitem = dict(((alias, prop.get_value(item))
                            for alias, prop in self.__mapping.items()))
            if self.sub_selects:
                sub_generators = tuple(
                    sub_select(item[prop]) for prop, sub_select
                    in self.sub_selects.items())
                for cartesian_item in itertools.product(*sub_generators):
                    for cartesian_atom in cartesian_item:
                        cartesian_atom.update(newitem)
                        yield cartesian_atom 
            else:
                yield newitem

    def validate(self, site, query, properties):
        def derive_property(prop, old_prop):
            if prop.child_property is None:
                return old_prop
            else:
                childname = prop.child_property.name
                try:
                    access_point = site.access_points[old_prop.remote_ap]
                    child_prop = access_point.properties[childname]
                except KeyError:
                    raise KeyError(
                        "This request has no %r property" % childname)
                return derive_property(prop.child_property, child_prop)
        new_props = {}
        for name, prop in query.mapping.items():
            old_prop = properties[prop.name]
            new_props[name] = derive_property(prop, old_prop)
        return True, new_props 


class QueryRange(Query):
    """Query selecting a range of items.

    >>> items = itertools.cycle([{'a':1, 'b':1},{'a':2, 'b': 2}])
    >>> range = QueryRange(slice(1,3))
    >>> list(range(items))
    [{'a': 2, 'b': 2}, {'a': 1, 'b': 1}]
    
    """
    def __init__(self, query_range):
        self.range = query_range

    def __call__(self, items):
        return itertools.islice(items, self.range.start, self.range.stop)

    def validate(self, site, query, properties):
        # TODO: validate self.range
        return True, properties
