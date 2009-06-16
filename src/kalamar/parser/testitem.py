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
        self._props = None
    
    def _read_property_from_data(self, prop_name):
        if self._props is None:
            from collections import defaultdict
            _props = defaultdict(lambda : None)
            self._open()
            data = self._stream.read()
            self._props.update(dict(zip(["album", "title"],
                                        data.split("\n",1))))
        return self._props[prop_name]
        
    def serialize(self):
        return self.properties["_content"]

del AtomItem
