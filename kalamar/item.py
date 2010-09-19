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


class MultiMapping(collections.Mapping):
    """A Mapping where each key as associated to multiple values.
    
    Stored values are actually tuples, but :meth:`__getitem__` only gives
    the first element of that tuple.
    
    To access the underlying tuples, use :meth:`getlist`.

    """
    @abstractmethod
    def getlist(self, key, value):
        raise KeyError

    def __getitem__(self, key):
        return self.getlist(key)[0]


class MutableMultiMapping(MultiMapping, collections.MutableMapping):
    """A mutable MultiMapping.
    
    Stored values are actually tuples, but :meth:`__getitem__` only gives
    the first element of that tuple.
    
    To access the underlying tuples, use :meth:`getlist` and :meth:`setlist`.

    """
    @abstractmethod
    def setlist(self, key, value):
        raise KeyError

    def __setitem__(self, key, value):
        self.setlist(key, (value,))
    
    def update(self, other):
        if isinstance(other, MultiMapping):
            for key in other:
                self.setlist(key, other.getlist(key))
        else:
            super(MutableMultiMapping, self).update(other)


class MultiDict(MutableMultiMapping):
    """Simple concrete subclass of MutableMultiMapping based on a dict.
    """
    def __init__(self):
        self.__data = {}
        
    def getlist(self, key):
        return self.__data[key]
    
    def setlist(self, key, values):
        self.__data[key] = tuple(values)

    def __delitem__(self, key):
        del self.__data[key]

    def __iter__(self):
        return iter(self.__data)

    def __len__(self):
        return len(self.__data)


class Item(MultiDict):
    """    
    :param access_point: The AccessPoint where this item came from.
    
    :param properties: A :class:`Mapping` of initial values for this item’s
        properties. May be a MultiMapping to have multiple values for a given
        property.
    :param lazy_loaders: A :class:`Mapping` of callable "loaders" for
        lazy properties. These callable should return a tuple of values.
        When loading a property is expensive, this allows to only load it
        when it’s needed.
        When you have only one value, wrap it in a tuple like this: `(value,)`
    
    Every property defined in the access point must be given in one of
    :obj:`properties` or :obj:`lazy_loaders`, but not both.

    """
    def __init__(self, access_point, properties=(), lazy_loaders=()):
        given_keys = set(properties)
        lazy_keys = set(lazy_loaders)
        ap_keys = set(access_point.properties)
        
        missing_keys = ap_keys - given_keys - lazy_keys
        if missing_keys:
            raise ValueError('Properties %r are neither given nor lazy.'
                             % (tuple(missing_keys),))
        intersection = given_keys & lazy_keys
        if intersection:
            raise ValueError('Properties %r are both given and lazy.'
                             % (tuple(intersection),))
        extra = given_keys - ap_keys
        if extra:
            raise ValueError('Unexpected given properties: %r'
                             % (tuple(extra),))
        extra = lazy_keys - ap_keys
        if extra:
            raise ValueError('Unexpected lazy properties: %r'
                             % (tuple(extra),))
        
        super(Item, self).__init__()
        self.access_point = access_point
        self._lazy_loaders = dict(lazy_loaders)
        self.update(properties)
        # update() sets modified to True, but we do not want initialisation
        # to count as a modification.
        self.modified = False
    
    def getlist(self, key):
        try:
            # TODO: not sure if super() is more appropriate here.
            return MultiDict.getlist(self, key)
        except KeyError:
            # KeyError (again) is expected here for keys not in
            # self.access_point.properties
            loader = self._lazy_loaders[key]
            values = loader()
            if not isinstance(values, tuple):
                raise ValueError('Lazy loaders must return a tuple, not %s. '
                    'To return a single value, wrap it in a tuple: (value,)'
                    % type(values))
            # TODO: not sure if super() is more appropriate here.
            MultiDict.setlist(self, key, values)
            del self._lazy_loaders[key]
            return values
    
    def setlist(self, key, values):
        # FIXME: This is here to avoid circular imports
        from .value import cast
        if key not in self:
            raise KeyError("%s object doesn't support adding new keys." %
                self.__class__.__name__)
        self.modified = True
        values = cast(self.access_point.properties[key], values)

        # TODO: not sure if super() is more appropriate here.
        MultiDict.setlist(self, key, values)
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

    def __contains__(self, key):
        # collections.Mutable’s default implementation is correct
        # but based on __getitem__ which may needlessly call a lazy loader.
        return key in self.access_point.properties

    def __repr__(self):
        """Return a user-friendly representation of item."""
        return "<%s(%s @ %s)>" % (
            self.__class__.__name__, repr(self.identity),
            repr(self.access_point.name))
    
    @property
    def identity(self):
        """Return an :class:`Identity` instance indentifying only this item."""
        ids = self.access_point.identity_properties
        return Identity(
            self.access_point.name, dict((name, self[name]) for name in ids))

    def save(self):
        """Save the item."""
        self.access_point.save(self)
        self.modified = False

    def delete(self):
        """Delete the item."""
        self.access_point.delete(self)
