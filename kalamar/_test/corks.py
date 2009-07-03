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

"""Some corks used for testing."""

import os
from ..item import Item, ItemProperties

class CorkItem(Item):
    
    aliases = {"I am aliased" : "I am not aliased"}
    format = "cork_item"
    
    def __init__(self, access_point, opener=None, storage_properties={}):
        self.properties = ItemProperties(self, storage_properties)
        
    def _parse_data(self):
        self.properties.update(
            {"I am not aliased" : "value of: I am not aliased",
            "cork_prop" : "I am a cork prop",
            "a" : "item's a",
            "b" : "item's b",
            "_content" : "item's raw data"})
        self.properties.setlistdefault("cork_prop").extend(["toto", "tata"])
        
    def serialize(self):
        return self.properties['_content']

class CorkAccessPoint:
    
    parser_aliases = {"I am aliased" : "I am not aliased"}
    storage_aliases = {}
    default_encoding = "utf-8"
    config = { "parser": "cork_item" }
    
    def get_storage_properties(self):
        return []
    
def cork_opener():
    return open(os.path.dirname(__file__) + "/toto")

