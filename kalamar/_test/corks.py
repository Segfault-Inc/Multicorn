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
from werkzeug import MultiDict, CombinedMultiDict

from ..item import Item

class CorkItem(Item):
    format = "cork_item"
    
    def __init__(self, access_point, opener=None, storage_properties={}):
        super(CorkItem, self).__init__(access_point, opener, storage_properties)
        self.aliases = {"I am aliased": "I am not aliased"}

    def _parse_data(self):
        self['I am not aliased'] = 'value of: I am not aliased'
        self['cork_prop'] = 'I am a cork prop'
        self['a'] = 'item\'s a'
        self['b'] = 'item\'s b'
        self.content = 'item\'s raw data'
        
    def serialize(self):
        return self.content

class CorkAccessPoint:
    parser_aliases = {"I am aliased": "I am not aliased"}
    storage_aliases = {}
    default_encoding = "utf-8"
    parser_name = "cork_item"
    
    def get_storage_properties(self):
        return []
    
def cork_opener():
    return open(os.path.dirname(__file__) + "/toto").read()

