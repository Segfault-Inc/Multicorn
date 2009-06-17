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
# along with Kalamar library.  If not, see <http://www.gnu.org/licenses/>.

"""TODO : put some doc here"""

from kalamar.item import AtomItem

class TestItem(AtomItem):
    """A class giving the raw (binary) access to an item's data"""
    
    format = "test_format"
    
    def __init__(self, access_point, opener, accessor_properties={}):
        super(TestItem, self).__init__(access_point, opener,
                                                        accessor_properties={})
    
    def _read_property_from_data(self, prop_name):
        aliased = [self.aliases_rev.get(name, name)
                   for name in ["album", "title"]]
        if key in aliased + ["_content"]:
            self._open()
            data = self._stream.read()
            self.properties["_content"] = data
            self.properties.update(dict(zip(aliased, data.split("\n",1))))
        else:
            self.properties[key] = None
        
    def serialize(self):
        album = self.properties[self.aliases_rev["album"]]
        title = self.properties[self.aliases_rev["title"]]
        return album+"\n"+title

del AtomItem
