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
import decimal
import uuid

from ..item import Item, ItemWrapper, ItemStub
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
    ItemClass = Item

    def __init__(self, properties, identity_properties):
        self.properties = {}
        for name, prop in properties.items():
            self.register(name, prop)
            if name in identity_properties:
                prop.mandatory = True
                prop.identity = True
        self._identity_property_names = identity_properties
        self.site = None
        self.name = None
        self.__identity_properties = None

    @staticmethod
    def _auto_value(prop):
        """Return a random value corresponding to ``prop`` type."""
        # TODO: find better random values
        # ``uuid4`` object *has* properties asked here
        # pylint: disable=E1101
        if prop.type == datetime.datetime:
            return (datetime.datetime.now(),)
        elif prop.type == datetime.date:
            return (datetime.date.today(),)
        elif prop.type == int:
            return (int(uuid.uuid4().time_low / 2),)
        elif prop.type == decimal.Decimal:
            return (decimal.Decimal(uuid.uuid4().time_low / 2),)
        elif prop.type == float:
            return (uuid.uuid4().int / float(uuid.uuid4().int),)
        elif prop.type == iter:
            return (uuid.uuid4().bytes,)
        else:
            return (prop.type(uuid.uuid4()),)
        # pylint: enable=E1101

    # This method can be overriden and should use ``self`` and ``properties``
    # pylint: disable=R0201
    # pylint: disable=W0613
    def _default_loader(self, properties, lazy_prop):
        """Return a default loader to manage references in an access point."""
        def loader(item):
            """Default loader for an access point."""
            condition = Condition(lazy_prop.remote_property.name, "=", item)
            return (list(lazy_prop.remote_ap.search(condition)),)
        return loader
    # pylint: enable=R0201
    # pylint: enable=W0613

    @property
    def identity_properties(self):
        """List of properties identifying the access point items."""
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

    def loader_from_reference_repr(self, representation):
        """Get a loader from the ``representation`` of the identity values.

        The ``representation`` is a string of slash-separated values
        representing the identity properties values, used as keys for
        heterogeneous linked access points.

        """
        if not representation:
            return lambda item: (None,)
        keys = [prop.name for prop in self.identity_properties]
        values = representation.split("/")
        if len(keys) != len(values):
            raise ValueError(
                "The representation doesn't match the identity properties.")
        properties = dict(zip(keys, values))
        return lambda item: (ItemStub(self, properties),)

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
            if prop.auto and name not in properties:
                if prop.auto is True:
                    function = lambda prop: lambda item: self._auto_value(prop)
                elif callable(prop.auto):
                    function = lambda prop: lambda item: prop.auto()
                elif isinstance(prop.auto, tuple):
                    function = lambda prop: lambda item: prop.auto
                else:
                    raise ValueError(
                        "Default values must be a tuple, not %s. To use a "
                        "single default value, wrap it in a tuple: (value,)."
                        % type(prop.auto).__name__)
                lazy_loaders[name] = function(prop)
            elif not prop.mandatory and name not in properties\
                    and name not in lazy_refs and name not in lazy_loaders:
                properties[name] = None

        for name, value in lazy_refs.items():
            lazy_loaders[name] = self._default_loader(properties, value)
        
        item = self.ItemClass(self, properties, lazy_loaders)
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

    def bind(self, site, name):
        """Link the access point to ``site`` and call it ``name``."""
        super(AccessPointWrapper, self).bind(site, name)
        if not self.wrapped_ap.site:
            self.wrapped_ap.bind(site, name)

