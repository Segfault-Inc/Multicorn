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
    
    protocol = 'sqlite'
    
    def get_connection(self):
        """Return (connection, table)
        
        Needs 'url' in the configuration in the following format:
            sqlite:///path/to/file?table
        or:
            sqlite://./rel/path/to/file?table
        or:
            sqlite://:memory:?table
        
        """
        if getattr(self, '_connection', None) is None:
            url = self.config['url']
            urldict = urlparse.urlsplit(url)
            splitted_path = urldict.path.split('?',1)
            file = splitted_path[0][2:]
            self._table = splitted_path[1]
            self._connection = sqlite3.connect(file)
        return (self._connection, self._table)
    
    def _get_primary_keys(self):
        """
        
        Fixture
        >>> from kalamar._test import fill_sqlite_db
        >>> storage = SQLiteStorage(url='sqlite://:memory:?test', basedir='')
        >>> conn, table = storage.get_connection()
        >>> fill_sqlite_db(conn)
        
        Test
        >>> storage._get_primary_keys()
        [u'key']
        
        """
        connection, table = self.get_connection()
        cursor = connection.cursor()
        cursor.execute('PRAGMA table_info(%s)' % table)
        # cid, name, type, notnull, dflt_value, pk
        
        fields = (dict(zip(['cid', 'name', 'type', 'notnull',
                            'dflt_value', 'pk'], values))
                  for values in cursor.fetchall())
        pkeys = [f['name'] for f in fields if f['pk']]
        return pkeys
