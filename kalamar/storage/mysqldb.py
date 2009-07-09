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
                decode_string = lambda c: c.decode(
                                    # Hack to convert python locale format to MySQL
                                    self.default_encoding.replace('-','')
                                )
    #            converters = {
    #                FIELD_TYPE.BIT: bool,
    #                FIELD_TYPE.BLOB: str,
    #                FIELD_TYPE.CHAR: decode_string,
    #                #FIELD_TYPE.DATE
    #                #FIELD_TYPE.DATETIME
    #                FIELD_TYPE.DECIMAL: float,
    #                FIELD_TYPE.DOUBLE: float,
    #                #FIELD_TYPE.ENUM
    #                FIELD_TYPE.FLOAT: float,
    #                #FIELD_TYPE.GEOMETRY
    #                FIELD_TYPE.INT24: int,
    #                #FIELD_TYPE.INTERVAL
    #                FIELD_TYPE.LONG: int,
    #                FIELD_TYPE.LONGLONG: int,
    #                FIELD_TYPE.LONG_BLOB: str,
    #                FIELD_TYPE.MEDIUM_BLOB: str,
    #                #FIELD_TYPE.NEWDATE
    #                FIELD_TYPE.NEWDECIMAL: float,
    #                #FIELD_TYPE.SET
    #                FIELD_TYPE.SHORT: int,
    #                FIELD_TYPE.STRING: decode_string,
    #                #FIELD_TYPE.TIME
    #                #FIELD_TYPE.TIMESTAMP
    #                FIELD_TYPE.TINY: int,
    #                FIELD_TYPE.TINY_BLOB: str,
    #                FIELD_TYPE.VARCHAR: decode_string,
    #                FIELD_TYPE.VAR_STRING: decode_string,
    #                FIELD_TYPE.YEAR: int
    #            }
    #            kwargs = {'conv': converters}
                kwargs = {}
                parts = self.config['url'].split('/')
                
                user_part, host_part = parts[2].split('@')
                kwargs['user'], kwargs['passwd'] = user_part.split(':')
                
                host_port = host_part.split(':')
                kwargs['host'] = host_port[0]
                if len(host_port) == 2:
                    kwargs['port'] = host_port[1]
                
                kwargs['db'], self._table = parts[3].split('?')
                
                self._connection = MySQLdb.connect(**kwargs)
                self._connection.set_character_set(
                    self.default_encoding.replace('-','')
                )

            return (self._connection, self._table)
        
        def _get_primary_keys(self):
            """Return the list of the table primary keys.
            
            Fixture
            #>>> from kalamar._test import fill_sqlite_db
            #>>> storage = SQLiteStorage(url='sqlite://:memory:?test', basedir='')
            #>>> connection, table = storage.get_connection()
            #>>> fill_sqlite_db(connection)
            
            Test
            #>>> storage._get_primary_keys()
            [u'key']
            
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
