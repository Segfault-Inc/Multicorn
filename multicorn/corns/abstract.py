# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ..property import BaseProperty
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


class AbstractCorn(object):

    Item = BaseItem
    Property = BaseProperty
    
    @classmethod
    def declarative(cls, class_):
        """
        Allow declarative instanciation, which in turn allows registration
        with a class decorator
        
            @metadata.register
            @SomeAccessPoint.declarative
            class pages:
                foo = 'bar'
        
        is the same as:
        
            pages = SomeAccessPoint(name='pages', foo='bar')
            metadata.register(pages)
        
        The declarative syntax may be more readable when arguments are many,
        long or deeply nested.
        """
        args = {'name': class_.__name__}
        args.update(
            (name, value) for name, value in vars(class_).iteritems()
            if not name.startswith('__'))
        return cls(**args)
        
    def __init__(self, name, properties, identity_properties):
        self.name = name
        self.multicorn = None
        self.identity_properties = tuple(identity_properties)
        if hasattr(properties, 'items'):
            # Assume a {name: type} dict
            self.properties = tuple(
                self.Property(name, type_)
                for name, type_ in properties.items())
        else:
            # Assume a sequence of Property objects
            self.properties = tuple(properties)

    def bind(self, multicorn):
        """
        Bind this access point to a Metadata.
        An access point can only be bound once.
        """
        if self.multicorn is None:
            self.multicorn = multicorn
        else:
            raise RuntimeError('This access point is already bound.')
    
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
    
    def search(self, query=None):
        """Execute the given query and return an iterable of items."""
        if query is None:
            # The empty query does nothing and gives all items.
            query = queries.Query
        return queries.execute(self._all(), query)

