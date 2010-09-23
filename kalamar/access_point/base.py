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
from itertools import product

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
    attribute is mandatory for :method:`view`.

    """
    __metaclass__ = abc.ABCMeta
    
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
    
    def view(self, view_request, **kwargs):
        """Return partial items.

        :param view_request: follows the same format as in the search method

        Example:
        site.view("access_point", {"name": "name", "boss_name": "foreign.name"})

        """
        def alias_item(item, aliases):
            return dict(
                (alias, item[value]) for alias, value in aliases.items())
        fake_props = []
        orphan_request = view_request.orphan_request
        # First, we perform a search on our own properties
        for item in self.search(view_request.request):
            join_request = And()
            view_item = alias_item(item, view_request.aliases)
            subitems_generators = []
            # Build subviews on our remote properties
            for prop, subview in view_request.subviews.items():
                property_obj = self.properties[prop]
                remote_ap = self.site.access_points[property_obj.remote_ap]
                if property_obj.relation == "many-to-one":
                    # Add aliases to the request to compare the item to our
                    # reference as well as a clause to the orphan_request to be
                    # tested against the resulting property
                    for id_prop in remote_ap.identity_properties:
                        fake_prop = "____" + prop + "____" + id_prop
                        fake_props.append(fake_prop)
                        join_request = And(
                            join_request, Condition(
                                fake_prop, "=", item[prop][id_prop]))
                        subview.aliases[fake_prop] = id_prop
                elif property_obj.relation == "one-to-many":
                    subview.additional_request = Condition(
                        property_obj.remote_property, "=", item)
                subitems_generators.append(remote_ap.view(subview))
            if not subitems_generators:
                yield view_item
            else:
                # Compute the cartesian product of the subviews results, update
                # the properties, and test it against the unevaluated request
                # before yielding
                for cartesian_item in product(*subitems_generators):
                    newitem = dict(view_item)
                    for cartesian_atom in cartesian_item:
                        newitem.update(cartesian_atom)
                        if orphan_request.test(newitem) \
                                and join_request.test(newitem):
                            for fake_prop in view_request.additional_aliases:
                                newitem.pop(fake_prop)
                            yield newitem
        
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
        item = Item(self, properties, lazy_loaders)
        item.modified = True
        return item

    def _default_loader(self, properties, lazy_prop):
        """Return a default loader to manage references in an access point."""
        remote = self.site.access_points[lazy_prop.remote_ap]
        if lazy_prop.relation == "one-to-many":
            id_props = self.identity_properties
            conditions = And(*[Condition(prop, "=", properties[prop])
                               for prop in id_props])
            def loader():
                return (remote.search(conditions),)
        else:
            raise RuntimeError(
                "Cannot use default lazy loader"
                "on %s relation" % lazy_prop.relation)
        return loader

    @abc.abstractmethod
    def save(self, item):
        """Update or add the item.

        This method has to be overriden.

        """
        raise NotImplementedError("Abstract method")


class AccessPointWrapper(AccessPoint):
    """A no-op access point wrapper. Meant to be subclassed."""
    
    # subclasses can override this.
    ItemWrapper = ItemWrapper
    
    def __init__(self, wrapped_ap):
        """Create an access point aliasing ``wrapped_ap`` properties.

        :param aliases: a dict where keys are the new property names,
            and values are the names in the wrapped access point.

        """
        super(AccessPointWrapper, self).__init__()
        self.wrapped_ap = wrapped_ap
        self.properties = wrapped_ap.properties
        self.identity_properties = wrapped_ap.identity_properties
    
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

