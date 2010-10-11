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

from .base import AccessPoint
from ..item import Item


class Memory(AccessPoint):
    """Trivial access point that keeps everything in memory.

    Mainly useful for testing.

    """
    def __init__(self, properties, id_property):
        super(Memory, self).__init__(properties, (id_property,))
        self._id_property = id_property
        self._store = {}
        
    def search(self, request):
        for properties in self._store.itervalues():
            item = Item(self, properties)
            if request.test(item):
                yield item
    
    def delete(self, item):
        del self._store[item[self._id_property]]
    
    def delete_many(self, request):
        # build a temporary list as we can not delete (change the dict size)
        # during iteration
        matching_items = list(self.search(request))
        for item in matching_items:
            self.delete(item)
    
    def save(self, item):
        self._store[item[self._id_property]] = dict(item)
    
