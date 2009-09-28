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
SQLite 3 access point.

This implementation depends on DBAPIStorage, the generic SQL database access
point.

"""

import warnings
try:
    import sqlite3
except ImportError:
    warnings.warn('Cannot import sqlite3. '
                  'SQLite3 support will not be available.')
else:
    import urlparse
    import os

    from dbapi import DBAPIStorage

    class SQLiteStorage(DBAPIStorage):
        """SQLite 3 access point"""
        protocol = 'sqlite'
        
        def get_db_module(self):
            return sqlite3
        
        def get_connection(self):
            """Return (``connection``, ``table``).
            
            Need 'url' in the configuration in the following format:
                sqlite:///path/to/file?table
            or:
                sqlite://./rel/path/to/file?table
            or:
                sqlite://:memory:?table
            
            """
            if not getattr(self, '_connection', None):
                url = self.config['url']
                url_dict = urlparse.urlsplit(url)
                splitted_path = url_dict.path.split('?', 1)
                file = splitted_path[0][2:]
                self._table = splitted_path[1]
                file = os.path.join(self.config['basedir'], file)
                self._connection = sqlite3.connect(file)

            return (self._connection, self._table)
        
        def _get_primary_keys(self):
            """Return the list of the table primary keys.
            
            Fixture
            >>> from kalamar._test import fill_sqlite_db
            >>> storage = SQLiteStorage(url='sqlite://:memory:?test', basedir='')
            >>> connection, table = storage.get_connection()
            >>> fill_sqlite_db(connection)
            
            Test
            >>> storage._get_primary_keys()
            [u'key']
            
            """
            connection, table = self.get_connection()
            cursor = connection.cursor()
            cursor.execute('PRAGMA table_info(%s)' % table)
            
            fields = (dict(zip(['cid', 'name', 'type', 'notnull',
                                'dflt_value', 'pk'], values))
                      for values in cursor.fetchall())

            return [field['name'] for field in fields if field['pk']]
