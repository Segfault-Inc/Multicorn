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

import collections
from werkzeug.datastructures import MultiDict, UpdateDictMixin


Identity = collections.namedtuple('Identity', 'access_point, conditions')


class Item(collections.MutableMapping):
    """Base class for items.
    
    The _access_point attribute represents where, in kalamar, the item is
    stored. It is an instance of AccessPoint.

    Items are hashable but mutable, use hash with caution.

    """
    def __init__(self, access_point, properties=None):
        self._access_point = access_point
        self.modified = False
        self._properties = {}

        for key, value in (properties or {}).items():
            self._properties[key] = (value,)
    
    def __getitem__(self, key):
        return self._properties[key][0]

    def __setitem__(self, key, value):
        self.modified = True
        self._properties[key] = (value,)

    def __delitem__(self, key):
        del self._properties[key]

    def __iter__(self):
        return iter(self._properties)

    def __len__(self):
        return len(self._properties)

    def __eq__(self, item):
        """Test if ``item`` is the same as this item."""
        if isinstance(item, Item):
            return hash(item) == hash(self)
        return NotImplemented

    def __cmp__(self, item):
        """Compare two items.
        
        Useful in some algorithms (sorting by key, for example).
        DO NOT USE UNLESS YOU KNOW WHAT YOU'RE DOING!
        
        """
        if isinstance(item, Item):
            return cmp(hash(self), hash(item))
        return NotImplemented

    def __repr__(self):
        """Return a user-friendly representation of item."""
        return "<%s(%s @ %s)>" % (
            self.__class__.__name__, repr(self.identity),
            repr(self._access_point.name))
    
    def __hash__(self):
        """Return a hash of item.
        
        Do not forget that items are mutable, so the hash could change!
        
        This hash value is useful in some algorithms (eg in sets) and it
        permits a huge gain of performance. However, DON'T USE THIS HASH UNLESS
        YOU KNOW WHAT YOU'RE DOING.
        
        """
        return hash(self._access_point.name + self.request)

    def setlist(self, key, values):
        self.modified = True
        self._properties[key] = tuple(values)

    def getlist(self, key):
        return self._properties[key]
    
    @property
    def identity(self):
        """Return an :class:`Identity` instance indentifying only this item."""
        return None

    def save(self):
        """Save the item."""
        self._access_point.save(self)
        self.modified = False

    def delete(self):
        """Delete the item."""
        self._access_point.delete(self)
