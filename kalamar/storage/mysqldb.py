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
                  'MySQL support will not be available.',
                  ImportWarning)
else:
    from MySQLdb.constants import FIELD_TYPE, CLIENT
    from MySQLdb import converters
    from copy import copy

    from kalamar.storage.dbapi import DBAPIStorage



    class MySQLdbStorage(DBAPIStorage):
        """MySQLdb access point"""
        protocol = 'mysql'
        
        # Key are those available in FIELD_TYPE.*
        # Here, the programmer was too lazy to find each litteral constants.
        identity = lambda x: x
        conversions = copy(converters.conversions)
        
        # This is the complete conversion dictionnary as found in converters.
        # Copy then unquote the line you want to modify.
        conversions.update({
            #types.IntType: converters.Thing2Str,
            #types.LongType: converters.Long2Int,
            #types.FloatType: converters.Float2Str,
            #types.NoneType: converters.None2NULL,
            #types.TupleType: converters.escape_sequence,
            #types.ListType: converters.escape_sequence,
            #types.DictType: converters.escape_dict,
            #types.InstanceType: converters.Instance2Str,
            #array.ArrayType: converters.array2Str,
            #types.StringType: converters.Thing2Literal, # default
            #types.UnicodeType: converters.Unicode2Str,
            #types.ObjectType: converters.Instance2Str,
            #types.BooleanType: converters.Bool2Str,
            #DateTimeType: converters.DateTime2literal,
            #DateTimeDeltaType: converters.DateTimeDelta2literal,
            #Set: converters.Set2Str,
            FIELD_TYPE.TINY: int,
            FIELD_TYPE.SHORT: int,
            FIELD_TYPE.LONG: long,
            FIELD_TYPE.FLOAT: float,
            FIELD_TYPE.DOUBLE: float,
            FIELD_TYPE.DECIMAL: float,
            FIELD_TYPE.NEWDECIMAL: float,
            FIELD_TYPE.LONGLONG: long,
            FIELD_TYPE.INT24: int,
            FIELD_TYPE.YEAR: int,
            FIELD_TYPE.SET: converters.Str2Set,
            FIELD_TYPE.TIMESTAMP: converters.mysql_timestamp_converter,
            FIELD_TYPE.DATETIME: converters.DateTime_or_None,
            FIELD_TYPE.TIME: converters.TimeDelta_or_None,
            FIELD_TYPE.DATE: converters.Date_or_None,
            #FIELD_TYPE.BLOB: [(FLAG.BINARY, str)],
            #FIELD_TYPE.STRING: [(FLAG.BINARY, str)],
            #FIELD_TYPE.VAR_STRING: [(FLAG.BINARY, str)],
            #FIELD_TYPE.VARCHAR: [(FLAG.BINARY, str)],
        })
        
        def get_db_module(self):
            """Return a DB-API implementation module."""
            return MySQLdb
        
        def _get_connection(self):
            """Return (``connection``, ``table``).
            
            Need 'url' in the configuration in the following format::

              mysql://user:password@host[:port]/base?table
            
            """
            kwargs = {}
            parts = self.config.url.split('/')
            
            user_part, host_part = parts[2].split('@')
            kwargs['user'], kwargs['passwd'] = user_part.split(':')
            
            host_port = host_part.split(':')
            kwargs['host'] = host_port[0]
            if len(host_port) == 2:
                kwargs['port'] = int(host_port[1])
            
            kwargs['db'], table = parts[3].split('?')
            
            kwargs['use_unicode'] = True
            #kwargs['conv'] = self.conversions
            kwargs['client_flag'] = CLIENT.FOUND_ROWS
            
            connection = self.get_db_module().connect(**kwargs)
            connection.set_sql_mode('ANSI')
            connection.set_character_set(
                # Hack to convert python locale format to MySQL
                self.default_encoding.replace('-',''))

            return connection, table
        
        @property
        def primary_keys(self):
            """List of the table primary keys."""
            connection, table = self.get_connection()
            cursor = connection.cursor()
            cursor.execute('DESCRIBE %s ;' % table)
            
            fields = (dict(zip(
                        ['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'],
                        values))
                      for values in cursor.fetchall())

            return [unicode(field['Field'])
                    for field in fields
                    if field['Key'] == 'PRI']
        
        @staticmethod
        def _quote_name(name):
            """Quote an SQL name (e.g. column name) with ` (grave accent).

            >>> MySQLdbStorage._quote_name('spam')
            '`spam`'

            """
            return '`%s`' % name
