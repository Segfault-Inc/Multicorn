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
    from pyPgSQL import PgSQL
except ImportError:
    warnings.warn('Cannot import pyPgSQL. '
                  'PostgreSQL support will not be available.')
else:
    import urlparse
    import os
    from time import sleep

    from dbapi import DBAPIStorage

    class PostgreSQLStorage(DBAPIStorage):
        """PostgreSQL access point"""
        protocol = 'postgres'
        
        def get_db_module(self):
            return PgSQL
        
        def get_connection(self):
            """Return (connection, table)
            
            Need 'url' in the configuration in the following format:
                postgres://user:password@host[:port]/base?table
            
            """
            if not getattr(self, '_connection', None):
                kwargs = {}
                parts = self.config['url'].split('/')
                
                user_part, host_part = parts[2].split('@')
                kwargs['user'], kwargs['password'] = user_part.split(':')
                
                host_and_port = host_part.split(':')
                kwargs['host'] = host_and_port[0]
                if len(host_and_port) == 2:
                    kwargs['port'] = host_and_port[1]
                
                kwargs['database'], self._table = parts[3].split('?')
                
                self._connection = DBAPI.connect(**kwargs)
                
            return (self._connection, self._table)
        
        def _get_primary_keys(self):
            """Return the list of the table primary keys.
            
            TODO test (possible ?)
            
            """
            connection, table = self.get_connection()
            cursor = connection.cursor()
            cursor.execute("""
                SELECT attr.attname, idx.indisprimary
                    FROM pg_catalog.pg_class c, pg_catalog.pg_class c2,
                        pg_catalog.pg_index idx, pg_catalog.pg_attribute attr
                    WHERE c.oid = idx.indrelid
                        AND idx.indexrelid = c2.oid
                        AND attr.attrelid = c.oid
                        AND attr.attnum = idx.indkey[0]
                        AND c.relname = %s;""", [table])

            return [field[0] for field in cursor.fetchall() if field[1]]

