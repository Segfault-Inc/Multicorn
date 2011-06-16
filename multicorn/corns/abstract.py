# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ..item import BaseItem
from .. import queries
from ..requests.types import Type, List, Dict
from ..python_executor import PythonExecutor
from ..requests.requests import StoredItemsRequest


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


class AbstractCorn(object):

    Item = BaseItem
    #TODO: fix and uncomment
    RequestWrapper = PythonExecutor

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
        self.properties[name] = Type(corn=self, name=name)

    def create(self, values=None, lazy_values=None):
        """Create and return a new item."""
        item = self.Item(self, values or {}, lazy_values or {})
        return item

    def open(self, request=None):
        """
        Same as search but return exactly one item.
        Raise if there are more or less than one result.
        """
        return self.all.filter(request).one().execute()

    # Minimal API for concrete access points

    def save(self, item):
        """Save an item"""
        raise NotImplementedError

    def delete(self, item):
        """Delete an item"""
        raise NotImplementedError

    def _all(self):
        """Return an iterable of all items in this access points."""
        raise NotImplementedError

    @property
    def all(self):
        """Return a request object representing the list of all items."""
        return StoredItemsRequest(self)

    @property
    def type(self):
        return Dict(corn=self, mapping=self.properties)

    # Can be overridden to optimize
    def execute(self, request):
        """Execute the given query and return an iterable of items."""
        wrapped_query = self.RequestWrapper.from_request(request)
        return wrapped_query.execute((self._all(),))
