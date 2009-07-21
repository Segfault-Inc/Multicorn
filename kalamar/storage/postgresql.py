# coding: utf8
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
    from pg8000 import dbapi as postg
except ImportError:
    warnings.warn('Cannot import pg8000. '
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
            return postg
        
        def get_connection(self):
            """Return (connection, table)
            
            Need 'url' in the configuration in the following format:
                postgres://user:password@host[:port]/base?table
            
            """
            if not hasattr(self, '_connection'):
                kwargs = {}
                parts = self.config['url'].split('/')
                
                user_part, host_part = parts[2].split('@')
                kwargs['user'], kwargs['password'] = user_part.split(':')
                
                host_and_port = host_part.split(':')
                kwargs['host'] = host_and_port[0]
                if len(host_and_port) == 2:
                    kwargs['port'] = int(host_and_port[1])
                
                kwargs['database'], self._table = parts[3].split('?')
                
                self._connection = self.get_db_module().connect(**kwargs)
                
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

            return [
                field[0].decode('utf-8') 
                for field in cursor.fetchall()
                if field[1]
            ]
        
        def _convert_parameters(self, parameters):
            description = self.get_table_description()
            module = self.get_db_module()
            
            conv_dict = {
                16: bool,
                17: module.Binary,
                19: unicode,
                20: long,
                21: int,
                23: int,
                25: unicode,
                #26: {'txt_in': <function numeric_in at 0x976217c>},
                700: float,
                701: float,
                829: unicode,
                #1000: {'bin_in': <function array_recv at 0x97623e4>},
                #1003: {'bin_in': <function array_recv at 0x97623e4>},
                #1005: {'bin_in': <function array_recv at 0x97623e4>},
                #1007: {'bin_in': <function array_recv at 0x97623e4>},
                #1009: {'bin_in': <function array_recv at 0x97623e4>},
                #1014: {'bin_in': <function array_recv at 0x97623e4>},
                #1015: {'bin_in': <function array_recv at 0x97623e4>},
                #1016: {'bin_in': <function array_recv at 0x97623e4>},
                #1021: {'bin_in': <function array_recv at 0x97623e4>},
                #1022: {'bin_in': <function array_recv at 0x97623e4>},
                1042: unicode,
                1043: unicode,
                #1082: {'txt_in': <function date_in at 0x976209c>},
                #1083: {'txt_in': <function time_in at 0x976210c>},
                #1114: {'bin_in': <function timestamp_recv at 0x9760f7c>},
                #1184: {'bin_in': <function timestamptz_recv at 0x9760fb4>},
                #1186: {'bin_in': <function interval_recv at 0x9762374>},
                #1231: {'bin_in': <function array_recv at 0x97623e4>},
                #1263: {'bin_in': <function array_recv at 0x97623e4>},
                #1700: {'bin_in': <function numeric_recv at 0x97621b4>},
                2275: unicode
            }
            new_parameters = []
            #prop_name is the same as the column name
            for parameter in parameters:
                if parameter.value is not None:
                    column_type_code = int(
                        description[parameter.name]['type_code']
                    )
                    converter = conv_dict[column_type_code]
                    parameter.value = converter(parameter.value)
                new_parameters.append(parameter)
            return new_parameters
