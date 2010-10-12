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
Access Point
============

Access point base class.

"""

import abc
import datetime
import uuid

from ..item import Item, ItemWrapper
from ..request import And, Condition


DEFAULT_PARAMETER = object()


class NotOneMatchingItem(Exception):
    """Not one object has been returned."""


class MultipleMatchingItems(NotOneMatchingItem):
    """More than one object have been returned."""


class ItemDoesNotExist(NotOneMatchingItem):
    """No object has been returned."""


class AccessPoint(object):
    """Abstract class for all access points.
    
    In addition to abstract methods and properties, concrete access points
    must have three attributes:
    
    :attr:`properties` is a dict where keys are
        property names as strings, and value are :class:`kalamar.property.Property`
        instances.
    :attr:`identity_properties` is a tuple of property names that compose
        the "identity" of items in this access point.

    Moreover, :attr:`site` is added when an access point is registered. This
    attribute is mandatory for :meth:`view`.

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, properties, identity_properties):
        self.properties = properties
        self.identity_properties = identity_properties

    @staticmethod
    def _auto_value(self, prop):
        """Return a random value corresponding to ``prop`` type."""
        if prop.type == datetime.datetime:
            # TODO: find a better random value
            return datetime.datetime.now()
        elif prop.type == datetime.date:
            # TODO: find a better random value
            return datetime.date.today()
        elif prop.type == float:
            return uuid.uuid4().int / float(uuid.uuid4().int)
        elif prop.type == iter:
            return uuid.uuid4().bytes
        else:
            return prop.type(uuid.uuid4())

    def _default_loader(self, properties, lazy_prop):
        """Return a default loader to manage references in an access point."""
        remote = self.site.access_points[lazy_prop.remote_ap]
        if lazy_prop.relation == "one-to-many":
            local_ref = self.identity_properties[0]
            condition_prop = "%s.%s" % (lazy_prop.remote_property, local_ref)
            conditions = Condition(condition_prop, '=', properties[local_ref])
        else:
            raise RuntimeError(
                "Cannot use default lazy loader"
                "on %s relation" % lazy_prop.relation)
        return lambda: (list(remote.search(conditions)),)

    def open(self, request, default=DEFAULT_PARAMETER):
        """Return the item in access point matching ``request``.
        
        If there is no result, raise ``Site.ObjectDoesNotExist``. If there are
        more than one result, raise ``Site.MultipleObjectsReturned``.
        
        """
        results = iter(self.search(request))
        try:
            item = results.next()
        except StopIteration:
            if default is DEFAULT_PARAMETER:
                raise ItemDoesNotExist
            return default
        try:
            results.next()
        except StopIteration:
            return item
        else:
            raise MultipleMatchingItems

    @abc.abstractmethod
    def search(self, request):
        """Return an iterable of every item matching request."""
        raise NotImplementedError("Abstract method")

    def view(self, view_query):
        """Return an iterable of dict-like objects matching ``view_query``.

        TODO: the real behaviour of this method should be explained

        """
        items = self.search(And())
        return view_query(items)

    def delete_many(self, request):
        """Delete all item matching ``request``."""
        for item in self.search(request):
            self.delete(item)
    
    @abc.abstractmethod
    def delete(self, item):
        """Delete ``item`` from the backend storage.
        
        This method has to be overridden.

        """
        raise NotImplementedError("Abstract method")
    
    def create(self, properties=None, lazy_loaders=None):
        """Create and return a new item."""
        properties = properties or {}
        lazy_loaders = lazy_loaders or {}
        lazy_refs = (
            dict([(name, prop) for name, prop in self.properties.items()
                  if prop.relation == "one-to-many"
                  and name not in properties and name not in lazy_loaders]))
        for name, value in lazy_refs.items(): 
            lazy_loaders[name] = self._default_loader(properties, value)

        # Create loaders for auto properties
        for name, prop in self.properties.items():
            if prop.auto and (name not in properties):
                lazy_loaders[name] = (
                    lambda prop: lambda: (self.auto_value(prop),))(prop)

        item = Item(self, properties, lazy_loaders)
        item.modified = True
        return item

    @abc.abstractmethod
    def save(self, item):
        """Update or add the item.

        This method has to be overriden.

        """
        raise NotImplementedError("Abstract method")


class AccessPointWrapper(AccessPoint):
    """A no-op access point wrapper. Meant to be subclassed."""
    # Subclasses can override this.
    ItemWrapper = ItemWrapper

    def __init__(self, wrapped_ap):
        """Create an access point aliasing ``wrapped_ap`` properties.

        :param aliases: a dict where keys are the new property names,
            and values are the names in the wrapped access point.

        """
        super(AccessPointWrapper, self).__init__(wrapped_ap.properties,
                wrapped_ap.identity_properties)
        self.wrapped_ap = wrapped_ap

    def search(self, request):
        for underlying_item in self.wrapped_ap.search(request):
            yield self.ItemWrapper(self, underlying_item)

    def delete_many(self, request):
        self.wrapped_ap.delete_many(request)

    def delete(self, item):
        self.wrapped_ap.delete(item.wrapped_item)

    def save(self, item):
        self.wrapped_ap.save(item.wrapped_item)

    def create(self, properties=None, lazy_loaders=None):
        underlying_item = self.wrapped_ap.create(properties, lazy_loaders)
        return self.ItemWrapper(self, underlying_item)
