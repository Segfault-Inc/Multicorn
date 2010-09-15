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
    def __init__(self, id_property='id'):
        self.id_property = id_property
        self._store = {}
        
    def search(self, request):
        return (Item(self, properties)
                for properties in self._store.itervalues()
                if request.test(properties))
    
    def delete(self, item):
        del self._store[item[self.id_property]]
    
    def save(self, item):
        self._store[item[self.id_property]] = dict(item)

