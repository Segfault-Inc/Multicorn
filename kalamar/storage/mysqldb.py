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
MySQL access point.

This implementation depends on DBAPIStorage, the generic SQL database access
point.

"""

import warnings
try:
    import MySQLdb
except ImportError:
    warnings.warn('Cannot import MySQLdb. '
                  'MySQL support will not be available.')
else:
    from MySQLdb import FIELD_TYPE
    import MySQLdb.constants.CLIENT
    import urlparse
    import os

    from dbapi import DBAPIStorage

    class MySQLdbStorage(DBAPIStorage):
        """MySQLdb access point"""
        protocol = 'mysql'
        
        def get_db_module(self):
            return MySQLdb
        
        def get_connection(self):
            """Return (connection, table)
            
            Need 'url' in the configuration in the following format:
                mysql://user:password@host[:port]/base?table
            
            """
            if not getattr(self, '_connection', None):
                kwargs = {}
                parts = self.config['url'].split('/')
                
                user_part, host_part = parts[2].split('@')
                kwargs['user'], kwargs['passwd'] = user_part.split(':')
                
                host_port = host_part.split(':')
                kwargs['host'] = host_port[0]
                if len(host_port) == 2:
                    kwargs['port'] = int(host_port[1])
                
                kwargs['db'], self._table = parts[3].split('?')
                
                kwargs['use_unicode'] = True
                kwargs['client_flag'] = MySQLdb.constants.CLIENT.FOUND_ROWS
                
                self._connection = MySQLdb.connect(**kwargs)
                self._connection.set_character_set(
                    # Hack to convert python locale format to MySQL
                    self.default_encoding.replace('-','')
                )
            self._connection.ping(True)
            return (self._connection, self._table)
        
        def _get_primary_keys(self):
            """Return the list of the table primary keys.
            
            TODO test (possible ?)
            
            """
            connection, table = self.get_connection()
            cursor = connection.cursor()
            cursor.execute('DESCRIBE %s;' % table)
            
            fields = (dict(zip(['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'], values))
                      for values in cursor.fetchall())

            return [unicode(field['Field'])
                    for field in fields
                    if field['Key']=='PRI']
        
        @staticmethod
        def _quote_name(name):
            """
            Quote an SQL name (e.g. table name, column name) with ` (grave accent).
            """
            return '`%s`' % name
