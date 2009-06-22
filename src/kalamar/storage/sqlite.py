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

from dbapi import DBAPIStorage
import sqlite3
import urlparse

class SQLiteStorage(DBAPIStorage):
    
    def get_connection(self):
        url = self.config['url']
        file = urlparse.urlsplit(url).path[2:]
        return sqlite3.connect(file)
    
    def _get_primary_keys(self):
        connection, table = self.get_connection()
        cursor.execute('PRAGMA table_info(%s)' % table)
        # cid, name, type, notnull, dflt_value, pk
        
        fields = (dict(zip(cursor.description, values))
                  for values in cursor.fetchall())
        pkeys = [field['name'] for field in fields if field[pk]]
        return pkeys
