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
Aliases access point
====================

Access point giving other names to the properties of the wrapped access point.

"""

from .base import AccessPoint
from ..item import MultiMapping, MultiDict, MutableMultiMapping
from ..request import Condition, And, Or, Not


class AliasedItem(MutableMultiMapping):
    def __init__(self, access_point, underlying_item):
        super(AliasedItem, self).__init__()
        self.access_point = access_point
        self.underlying_item = underlying_item

    def __repr__(self):
        return "AliasedItem(%r, %r)" % (
            self.access_point, self.underlying_item)
    
    def _translate_key(self, key):
        if key not in self.access_point.properties:
            raise KeyError
        return self.access_point.aliases.get(key, key)
    
    def getlist(self, key):
        return self.underlying_item.getlist(self._translate_key(key))
    
    def setlist(self, key, values):
        return self.underlying_item.setlist(self._translate_key(key), values)
    
    def __delitem__(self, key):
        raise TypeError("%s object doesn't support item deletion." %
            self.__class__.__name__)

    def __iter__(self):
        for key in self.underlying_item:
            yield self.access_point.reversed_aliases.get(key, key)

    def __len__(self):
        return len(self.underlying_item)

    # default to underlying_item for all other methods and attributes
    def __getattr__(self, name):
        return getattr(self.underlying_item, name)


class Aliases(AccessPoint):
    """Wrapper access point renaming properties."""
    def __init__(self, aliases, wrapped_ap):
        """Create an access point aliasing ``wrapped_ap`` properties.

        :param aliases: a dict where keys are the new property names,
            and values are the names in the wrapped access point.

        """
        super(Aliases, self).__init__()
        self.wrapped_ap = wrapped_ap
        self.aliases = aliases
        self.reversed_aliases = dict((v, k) for k, v in self.aliases.items())
        self.properties = dict(
            (self.reversed_aliases.get(name, name), property)
            for name, property in wrapped_ap.properties.items())
        self.identity_properties = tuple(
            self.reversed_aliases.get(name, name)
            for name in wrapped_ap.identity_properties)
    
    def translate_request(self, request):
        if isinstance(request, And):
            return And(*(self.translate_request(r)
                         for r in request.sub_requests))
        elif isinstance(request, Or):
            return Or(*(self.translate_request(r)
                        for r in request.sub_requests))
        elif isinstance(request, Not):
            return Not(self.translate_request(request.sub_request))
        elif isinstance(request, Condition):
            name = request.property_name
            return Condition(self.aliases.get(name, name),
                             request.operator,
                             request.value)
        else:
            raise ValueError("Unknown request type : %r" % request)
    
    def search(self, request):
        request = self.translate_request(request)
        for underlying_item in self.wrapped_ap.search(request):
            yield AliasedItem(self, underlying_item)
    
    def delete_many(self, request):
        self.wrapped_ap.delete_many(self.translate_request(request))
    
    def delete(self, item):
        self.wrapped_ap.delete(item.underlying_item)
    
    def save(self, item):
        self.wrapped_ap.save(item.underlying_item)
    
    def create(self, properties=None, lazy_loaders=None):
        if properties:
            if isinstance(properties, MultiMapping):
                props = MultiDict()
                for key in properties:
                    props.setlist(self.aliases.get(key, key), 
                        properties.getlist(key))
            else:
                props = dict((self.aliases.get(key, key), value)
                    for key, value in dict(properties).iteritems())
        if lazy_loaders:
            lazy_loaders = dict(
                (self.aliases.get(key, key), value)
                for key, value in dict(lazy_loaders).iteritems())
        return AliasedItem(self, self.wrapped_ap.create(props, lazy_loaders))
