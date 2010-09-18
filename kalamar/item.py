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
Item
====

Base classes to create kalamar items.

"""

from abc import abstractmethod
import collections


Identity = collections.namedtuple('Identity', 'access_point, conditions')


class MutableMultiMapping(collections.MutableMapping):
    """A MutableMapping where each key as associated to multiple values.
    
    Stored values are actually tuples, but :meth:`__getitem__` only gives
    the first element of that tuple, and :meth:`__setitem__` wraps the new 
    value in a tuple.
    
    To access the underlying tuples, use :meth:`getlist` and :meth:`setlist`.

    """
    @abstractmethod
    def getlist(self, key, value):
        raise KeyError

    @abstractmethod
    def setlist(self, key, value):
        raise KeyError

    def __getitem__(self, key):
        return self.getlist(key)[0]

    def __setitem__(self, key, value):
        self.setlist(key, (value,))


class Item(MutableMultiMapping):
    """Base class for items.
    
    The :attr:`access_point` attribute represents where, in kalamar, 
    the item is stored. It is an instance of :class:`AccessPoint`.

    """
    def __init__(self, access_point, properties=(), lazy_loaders=()):
        given_keys = set(properties)
        lazy_keys = set(lazy_loaders)
        
        missing_keys = set(access_point.properties) - given_keys - lazy_keys
        if missing_keys:
            raise ValueError('Properties %r are neither given nor lazy.'
                             % (tuple(missing_keys),))
        intersection = given_keys & lazy_keys
        if intersection:
            raise ValueError('Properties %r are both given and lazy.'
                             % (tuple(intersection),))
        
        self.access_point = access_point
        self._lazy_loaders = dict(lazy_loaders)
        self._properties = {}
        self.update(properties or {})
        # update() sets modified to True, but we do not want initialisation to
        # count as a modification.
        self.modified = False
    
    def getlist(self, key):
        try:
            return self._properties[key]
        except KeyError:
            value = self._lazy_loaders[key]
            self._properties[key] = value
            del self._lazy_loaders[key]
            return value
    
    def setlist(self, key, values):
        self.modified = True
        self._properties[key] = tuple(values)
        try:
            del self._lazy_loaders[key]
        except KeyError:
            pass

    def __delitem__(self, key):
        raise TypeError("%s object doesn't support item deletion." %
            self.__class__.__name__)

    def __iter__(self):
        return iter(self.access_point.properties)

    def __len__(self):
        return len(self.access_point.properties)

    def __repr__(self):
        """Return a user-friendly representation of item."""
        return "<%s(%s @ %s)>" % (
            self.__class__.__name__, repr(self.identity),
            repr(self.access_point.name))
    
    @property
    def identity(self):
        """Return an :class:`Identity` instance indentifying only this item."""
        ids = self.access_point.identity_properties
        return Identity(self.access_point.name, 
                        dict(name, self[name] for name in ids))

    def save(self):
        """Save the item."""
        self.access_point.save(self)
        self.modified = False

    def delete(self):
        """Delete the item."""
        self.access_point.delete(self)
