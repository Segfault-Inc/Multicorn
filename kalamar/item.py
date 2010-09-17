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
Base classes to create kalamar items.

"""

from abc import abstractmethod
import collections
from werkzeug.datastructures import MultiDict, UpdateDictMixin


Identity = collections.namedtuple('Identity', 'access_point, conditions')


class MutableMultiMapping(collections.MutableMapping):
    """A MutableMapping where each key as associated to multiple values.
    
    Stored values are actually tuples, but __getitem__ only gives the first
    element of that tuple, and __setitem__ wraps the new value in a tuple.
    
    To access the underlying tuples, use getlist and setlist.

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
    
    The _access_point attribute represents where, in kalamar, the item is
    stored. It is an instance of AccessPoint.

    Items are hashable but mutable, use hash with caution.

    """
    def __init__(self, access_point, properties=None):
        self._access_point = access_point
        self._properties = {}
        self.update(properties or {})
        # update() sets modified to True, but we do not want initialisation to
        # count as a modification.
        self.modified = False
    
    def __delitem__(self, key):
        del self._properties[key]

    def __iter__(self):
        return iter(self._properties)

    def __len__(self):
        return len(self._properties)

    def __repr__(self):
        """Return a user-friendly representation of item."""
        return "<%s(%s @ %s)>" % (
            self.__class__.__name__, repr(self.identity),
            repr(self._access_point.name))
    
    def setlist(self, key, values):
        if key not in self:
            raise KeyError(key)
        self.modified = True
        self._properties[key] = tuple(values)

    def getlist(self, key):
        return self._properties[key]
    
    @property
    def identity(self):
        """Return an :class:`Identity` instance indentifying only this item."""
        return NotImplemented

    def save(self):
        """Save the item."""
        self._access_point.save(self)
        self.modified = False

    def delete(self):
        """Delete the item."""
        self._access_point.delete(self)
