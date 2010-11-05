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
    value = __doc__


class MultipleMatchingItems(NotOneMatchingItem):
    """More than one object have been returned."""
    value = __doc__


class ItemDoesNotExist(NotOneMatchingItem):
    """No object has been returned."""
    value = __doc__


class AlreadyRegistered(RuntimeError):
    """Access point is already registered so a site."""
    value = __doc__


class AccessPoint(object):
    """Abstract class for all access points.
    
    In addition to abstract methods and properties, concrete access points
    must have two attributes:
    
    :param properties: Mapping where keys are property names as strings, and
        value are :class:`kalamar.property.Property` instances.
    :param identity_properties: Tuple of property names that compose the
        "identity" of items in this access point.

    Moreover, :attr:`site` is added when an access point is registered. This
    attribute is mandatory for :meth:`view`.

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, properties, identity_properties):
        self.properties = {}
        for name, prop in properties.items():
            self.register(name, prop)
        self._identity_property_names = identity_properties
        self.site = None
        self.name = None
        self.__identity_properties = None

    @staticmethod
    def _auto_value(prop):
        """Return a random value corresponding to ``prop`` type."""
        if prop.type == datetime.datetime:
            # TODO: find a better random value
            return datetime.datetime.now()
        elif prop.type == datetime.date:
            # TODO: find a better random value
            return datetime.date.today()
        elif prop.type == float:
            # ``uuid4`` object *has* an ``int`` property
            # pylint: disable=E1101
            return uuid.uuid4().int / float(uuid.uuid4().int)
            # pylint: enable=E1101
        elif prop.type == iter:
            return uuid.uuid4().bytes
        else:
            return prop.type(uuid.uuid4())

    def _default_loader(self, properties, lazy_prop):
        """Return a default loader to manage references in an access point."""
        local_ref = self.identity_properties[0]
        condition_prop = "%s.%s" % (lazy_prop.remote_property, local_ref.name)
        conditions = Condition(condition_prop, "=", local_ref)
        return lambda: (list(lazy_prop.remote_ap.search(conditions)),)

    @property
    def identity_properties(self):
        if not self.__identity_properties:
            self.__identity_properties = []
            for name in self._identity_property_names:
                self.__identity_properties.append(self.properties[name])
        return self.__identity_properties

    def bind(self, site, name):
        """Link the access point to ``site`` and call it ``name``."""
        if not self.site and not self.name:
            self.site = site
            self.name = name
        else:
            raise AlreadyRegistered

    def register(self, name, prop):
        """Add a property to this access point.

        :param name: Identifier string of the added property.
        :param prop: Instance of :class:`Property`.

        """
        self.properties[name] = prop
        prop.bind(self, name)

    def open(self, request, default=DEFAULT_PARAMETER):
        """Return the item in access point matching ``request``.
        
        If there is no result, raise :exc:`Site.ObjectDoesNotExist`. If there
        are more than one result, raise :exc:`Site.MultipleObjectsReturned`.

        If there is no result but the ``default`` parameter is given,
        ``default`` is returned.
        
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
        raise NotImplementedError

    def view(self, view_query):
        """Return an iterable of dict-like objects matching ``view_query``.

        TODO: the real behaviour of this method should be explained

        """
        items = (dict(item) for item in self.search(And()))
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
        raise NotImplementedError
    
    def create(self, properties=None, lazy_loaders=None):
        """Create and return a new item."""
        properties = properties or {}
        lazy_loaders = lazy_loaders or {}
        lazy_refs = (
            dict([(name, prop) for name, prop in self.properties.items()
                  if prop.relation == "one-to-many"
                  and name not in properties and name not in lazy_loaders]))

        # Create loaders for auto properties
        for name, prop in self.properties.items():
            if prop.auto and (name not in properties):
                properties[name] = self._auto_value(prop)

        for name, value in lazy_refs.items(): 
            lazy_loaders[name] = self._default_loader(properties, value)

        item = Item(self, properties, lazy_loaders)
        item.modified = True
        item.saved = False
        return item

    @abc.abstractmethod
    def save(self, item):
        """Update or add the item.

        This method has to be overriden.

        """
        raise NotImplementedError


class AccessPointWrapper(AccessPoint):
    """A no-op access point wrapper. Meant to be subclassed."""
    # Subclasses can override this.
    ItemWrapper = ItemWrapper

    def __init__(self, wrapped_ap):
        """Create an access point aliasing ``wrapped_ap`` properties.

        :param aliases: a dict where keys are the new property names,
            and values are the names in the wrapped access point.

        """
        copied_properties = dict(
            (name, prop.copy()) for name, prop in wrapped_ap.properties.items())
        copied_identity_properties = [
            prop.name for prop in wrapped_ap.identity_properties]
        super(AccessPointWrapper, self).__init__(
            copied_properties, copied_identity_properties)
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
