# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from ..item import BaseItem
from .. import queries


# Just a base class for the two other in case you want to catch both.
class NotOneMatchingItem(Exception):
    """
    Exactly one item was expected but a different number (zero or more than
    one) came.
    """
    value = __doc__


class MultipleMatchingItems(NotOneMatchingItem):
    """More than one object have been returned."""
    value = __doc__


class ItemDoesNotExist(NotOneMatchingItem):
    """No object has been returned."""
    value = __doc__


class Property(object):

    def __init__(self, corn=None, path=None, type=type):
        self.corn = corn
        self.path = path or ()
        self.type = type

class Dict(Property):

    def __init__(self, corn=None, path=None, mapping=None):
        super(Dict, self).__init__(corn=corn, path=path, type=dict)
        self.mapping = mapping

class List(Property):

    def __init__(self, corn=None, path=None, inner_type=Property(type=object)):
        super(Dict, self).__init__(corn=corn, path=path, type=list)
        self.inner_type = inner_type

class AbstractCorn(object):

    Item = BaseItem
    #TODO: fix and uncomment
    #RequestWrapper = PythonRequestWrapper

    def __init__(self, name, identity_properties):
        self.name = name
        self.multicorn = None
        self.identity_properties = tuple(identity_properties)
        self.properties = {}

    def bind(self, multicorn):
        """
        Bind this access point to a Metadata.
        An access point can only be bound once.
        """
        if self.multicorn is None:
            self.multicorn = multicorn
        else:
            raise RuntimeError('This access point is already bound.')

    def register(self, name, **kwargs):
        """
        Register a property within this corn.
        AbstractCorn just assumes the type object
        """
        self.properties[name] = Property(corn=self, path=(name,))


    def create(self, values=None, lazy_values=None, save=True):
        """Create and return a new item."""
        item = self.Item(self, values or {}, lazy_values or {})
        if save:
            self.save(item)
        return item

    def open(self, query=None):
        """
        Same as search but return exactly one item.
        Raise if there are more or less than one result.
        """
        results = iter(self.search(query))
        try:
            item = next(results)
        except StopIteration:
            raise ItemDoesNotExist
        try:
            next(results)
        except StopIteration:
            return item
        else:
            raise MultipleMatchingItems

    # Minimal API for concrete access points

    def save(self, item):
        """Return an iterable of all items in this access points."""
        raise NotImplementedError

    def _all(self):
        """Return an iterable of all items in this access points."""
        raise NotImplementedError

    # Can be overridden to optimize

    def execute(self, request):
        """Execute the given query and return an iterable of items."""
        wrapped_query = self.RequestWrapper.wrap(request)
        return wrapped_query.execute(self._all())
