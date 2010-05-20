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
                  'SQLite3 support will not be available.',
                  ImportWarning)
else:
    import os

    from kalamar.storage.dbapi import DBAPIStorage



    class SQLiteStorage(DBAPIStorage):
        """SQLite 3 access point"""
        protocol = 'sqlite'
        
        def get_db_module(self):
            return sqlite3
        
        @staticmethod
        def url_to_filename_table(basedir, url):
            """Return the filename and the table to use for the SQLite DB
            from a kalamar access point url.
            
            >>> f = SQLiteStorage.url_to_filename_table
            >>> f('/base', 'sqlite://test.db?table')
            ('/base/test.db', 'table')
            >>> f('/base', 'sqlite:///path/to/test.db?table') # one more slash
            ('/path/to/test.db', 'table')
            """
            protocal, remainder = url.split(':', 1)
            assert remainder.startswith('//')
            filename, table = remainder[2:].split('?', 1)
            filename = os.path.join(basedir, filename)
            return filename, table
            
        
        def _get_connection(self):
            """Return (``connection``, ``table``).
            
            Need 'url' in the configuration in the following format:
                sqlite:///path/to/file?table
            or:
                sqlite://./rel/path/to/file?table
            or:
                sqlite://:memory:?table
            
            """
            filename, table = self.url_to_filename_table(self.config.basedir,
                                                         self.config.url)
            connection = sqlite3.connect(filename)
            return connection, table
        
        @property
        def primary_keys(self):
            """List of the table primary keys.
            
            Fixture
            >>> from kalamar._test import fill_sqlite_db
            >>> from kalamar.config import Config
            >>> storage = SQLiteStorage(Config('sqlite://:memory:?test','',{},basedir=''))
            >>> connection, table = storage.get_connection()
            >>> fill_sqlite_db(connection)
            
            Test
            >>> storage.primary_keys
            [u'key']
            
            """
            connection, table = self.get_connection()
            cursor = connection.cursor()
            cursor.execute('PRAGMA table_info(%s)' % table)
            
            fields = (dict(zip(['cid', 'name', 'type', 'notnull',
                                'dflt_value', 'pk'], values))
                      for values in cursor.fetchall())

            return [field['name'] for field in fields if field['pk']]
