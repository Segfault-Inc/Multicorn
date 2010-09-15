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
from werkzeug import MultiDict
from werkzeug.contrib.sessions import ModificationTrackingDict


Identity = collections.namedtuple('Identity', 'access_point, conditions')


class Item(MultiDict, ModificationTrackingDict):
    """Base class for items.
    
    The _access_point attribute represents where, in kalamar, the item is
    stored. It is an instance of AccessPoint.

    Items are hashable but mutable, use hash with caution.

    """
    def __init__(self, access_point, properties={}):
        self._access_point = access_point
        self._old_properties = None
        self.modified = True
        self._loaded_properties = MultiDict()

        item_properties = MultiDict(
            (name, None) for name in access_point.properties)
        if properties:
            for name, value in properties.items():
                # TODO: manage MultiDicts
                item_properties[name] = value
        
        self.update(item_properties)

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
            str1 = hash(self)
            str2 = hash(item)
            return cmp(str1, str2)
        return NotImplemented

    def __repr__(self):
        """Return a user-friendly representation of item."""
        return "<%s(%s @ %s)>" % (
            self.__class__.__name__, repr(self.request),
            repr(self.access_point_name))
    
    def __hash__(self):
        """Return a hash of item.
        
        Do not forget that items are mutable, so the hash could change!
        
        This hash value is useful in some algorithms (eg in sets) and it
        permits a huge gain of performance. However, DON'T USE THIS HASH UNLESS
        YOU KNOW WHAT YOU'RE DOING.
        
        """
        return hash(self.access_point_name + self.request)

    @property
    def encoding(self):
        """Return the item encoding.

        Return the item encoding, based on what the parser can know from
        the item data or, if unable to do so, on what is specified in the
        access_point.

        """
        return self._access_point.default_encoding
    
    @property
    def identity(self):
        """Return an :class:`Identity` instance indentifying only this item."""
        return 

    def save(self):
        """Save the item."""
        self.access_point._save(item)

    def delete(self):
        """Delete the item."""
        self.access_point._delete(item)
