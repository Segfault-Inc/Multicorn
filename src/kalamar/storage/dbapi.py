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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

from kalamar.storage.base import AccessPoint

def file_opener(filename):
    # TODO : Change these lines
    #def _opener():
    #    return open(filename, 'rb')
    _opener = ""
    return _opener
        

class DBAPIStorage(AccessPoint):
    """Base class for SQL SGBD Storage"""
    
    protocol = None
    
    def __init__(self, **config):
        # connect to DB in self._connection
        # store table in self._table
        super(DBAPIStorage, self).__init__(**config)
    
    def get_storage_properties(self):
        cur = self._connection.cursor()
        #Empty request to get columns names
        #cur.execute('select * from %s where 0'%)
        return [prop[0] for prop in cur.description]
    
    def _storage_search(self, conditions):
        raise NotImplementedError # TODO
    
    def _format_request(self, request, parameters=tuple()):
        """Return a tuple (formated_request, typed_parameters).
        
        ``request'' must be in the DB-API's 'qmark' style.
        ``parameters'' is a tuple of strings.
        
        The returned ``formated_request'' is in the DB-API 2 style given by
        paramstyle (see DB-API spec.).
        
        """
        style = connection.paramstyle
        if style == 'qmark':
            return (request, parameters)
        elif style =='numeric':
            request = [enumerate(request.split('?'))]
            self._numeric_format_request(request, parameters)
        elif style == 'named':
            self._named_format_request(request, parameters)
        elif style == 'format':
            self._format_format_request(request, parameters)
        elif style == 'pyformat':
            self._pyformat_format_request(request, parameters)
        
        
            
    def save(self, item):
        raise NotImplementedError # TODO

    def remove(self, item):
        raise NotImplementedError # TODO
