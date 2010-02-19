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
Some corks used for testing.

"""

import os

from kalamar import Item
from kalamar.storage.dbapi import DBAPIStorage


class CorkDBAPIStorage(DBAPIStorage):
    class db_mod(object):
        BINARY = 1
        DATETIME = 1
        def Binary(self, data):
            return data

    @property
    def primary_keys(self):
        return ['pk1', 'pk2']

    def get_db_module(self):
        return self.db_mod()

    def get_table_description(self):
        return {
            'sto_prop': {'type_code': 1},
            'sto_prop2': {'type_code': 1},
            'pk1': {'type_code': 1},
            'pk2': {'type_code': 1},
            'content_col': {'type_code': 1}}



class CorkItem(Item):
    """Testing item."""
    format = "cork_item"
    
    def __init__(self, access_point, opener=None, storage_properties={}):
        super(CorkItem, self).__init__(access_point, opener, storage_properties)
        self.aliases = {"I am aliased": "I am not aliased"}
        self._raw_content = 'item\'s raw data'

    def _parse_data(self):
        self['I am not aliased'] = 'value of: I am not aliased'
        self['cork_prop'] = 'I am a cork prop'
        self['a'] = 'item\'s a'
        self['b'] = 'item\'s b'
        
    def serialize(self):
        return self._get_content()



class CorkAccessPoint:
    """Testing access point."""
    parser_aliases = {"I am aliased": "I am not aliased"}
    storage_aliases = {}
    default_encoding = "utf-8"
    parser_name = "cork_item"
    
    def get_storage_properties(self):
        return []
    


def cork_opener():
    """Open and return testing file."""
    return open(os.path.dirname(__file__) + "/toto").read()
