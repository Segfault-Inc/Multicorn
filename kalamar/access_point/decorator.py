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
Decorator
=========

Abstract access point simplifying the development of access points offering new
properties derived from old ones.

"""

import abc

from . import AccessPointWrapper
from ..item import ItemWrapper, MultiDict
from ..request import And
from ..property import Property


class DecoratorItem(ItemWrapper):
    """Item with additional properties."""
    def __init__(self, access_point, wrapped_item, decorated_values=None):
        super(DecoratorItem, self).__init__(access_point, wrapped_item)
        self.unsaved_properties = decorated_values or MultiDict()

    def getlist(self, key):
        if key in self.decorated_properties:
            try:
                return self.unsaved_properties.getlist(key)
            except KeyError:
                return self.decorated_properties[key].getter(self)
        else:
            return super(DecoratorItem, self).getlist(key)

    @property
    def decorated_properties(self):
        """List of additional properties."""
        return self.access_point.decorated_properties

    def setlist(self, key, values):
        if key in self.decorated_properties:
            return self.unsaved_properties.setlist(key, values)
        else:
            super(DecoratorItem, self).setlist(key, values)


class DecoratorProperty(Property):
    """Property suitable for a decorator access point."""
    def __init__(self, property_type, getter, *args, **kwargs):
        super(DecoratorProperty, self).__init__(property_type, *args, **kwargs)
        self.getter = getter


class Decorator(AccessPointWrapper):
    """Access point adding new properties to another access point."""
    __metaclass__ = abc.ABCMeta
    ItemDecorator = DecoratorItem

    def __init__(self, wrapped_ap, decorated_properties):
        super(Decorator, self).__init__(wrapped_ap)
        self.decorated_properties = decorated_properties
        for key, prop in self.decorated_properties.items():
            self.register(key, prop)

    def search(self, request):
        # The search request is quite special, since we can't rely on
        # the underlying access point if any of our decorated properties
        # is present in the request.
        tree = request.properties_tree
        if any((key in tree for key in self.decorated_properties)):
            for item in self.wrapped_ap.search(And()):
                decorated_item = self.ItemDecorator(self, item)
                if request.test(decorated_item):
                    yield decorated_item
        else:
            for item in self.wrapped_ap.search(request):
                yield self.ItemDecorator(self, item)

    def delete_many(self, request):
        # Once again, we can't rely on the underlying access point.
        # However, it is quite nice to benefit from its optimizations
        # when possible.
        tree = request.properties_tree
        if any((key in tree for key in self.decorated_properties)):
            for item in set(self.search(request)):
                self.wrapped_ap.delete(item.wrapped_item)
        else:
            self.wrapped_ap.delete_many(request)

    def save(self, item):
        self.preprocess_save(item)
        self.wrapped_ap.save(item.wrapped_item)
        item.unsaved_properties = MultiDict()

    @abc.abstractmethod
    def preprocess_save(self, item):
        """Preprocess a wrapped item, updating its content."""
        raise NotImplementedError

    def create(self, properties=None, lazy_loaders=None):
        decorated_values = MultiDict()
        properties = MultiDict(properties or {})
        for key in dict(properties):
            if key in dict(self.decorated_properties):
                values = properties.getlist(key)
                del properties[key]
                decorated_values.setlist(key, values)
        underlying_item = self.wrapped_ap.create(properties, lazy_loaders)
        return self.ItemDecorator(self, underlying_item, decorated_values)
