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
Database access point.

DBAPIStorage is just a base class to construct different databases access
points.

"""

from array import array
from copy import deepcopy
from itertools import izip
from StringIO import StringIO

from kalamar.storage.base import AccessPoint
from kalamar import utils
from kalamar import Item

_opener = lambda content: (lambda: StringIO(content))

class DBAPIStorage(AccessPoint):
    """Base class for SQL SGBD Storage.
    
    Descendant class must override "get_connection", "_get_primary_keys",
    "get_db_module" and "protocol".

    It may be useful to also redefine the following methods or attributes:
    - _quote_name
    - _sql_escape_quotes
    - sql_operators
    
    """
    protocol = None
    
    class UnsupportedParameterStyleError(Exception): pass
    class ManyItemsUpdatedError(Exception): pass
    
    # Provided to ensure compatibility with as many SGDB as possible.
    # This may be modified by a descendant.
    sql_operators = {
        '=': '=',
        '!=': '!=',
        '>': '>',
        '>=': '>=',
        '<': '<',
        '<=': '<=',
        }
    
    def save(self, item):
        """Save item in the database."""
        connection, table = self.get_connection()
        
        style = self.get_db_module().paramstyle
        
        request, parameters = self._build_update_request(table, item, style)
        
        cursor = connection.cursor()
        try:
            cursor.execute(request, parameters)
    
            if cursor.rowcount == 0:
                # Item does not exist, let's do an insert
                request, parameters = self._build_insert_request(table, item,
                                                                 style)
                cursor.execute(request, parameters)
            else:
                assert cursor.rowcount == 1

            connection.commit()
        finally:
            cursor.close()

    def remove(self, item):
        """Remove the item from the database."""
        connection, table = self.get_connection()
        table = self._sql_escape_quotes(table)
        request = array('u', u"DELETE FROM %s WHERE " % self._quote_name(table))
        
        primary_keys = self._get_primary_keys()
        parameters = []
        
        for key in primary_keys[:-1]:
            request.extend("%s=? AND " % self._quote_name(key))
            parameters.append((key, item.properties[key]))
        request.extend("%s=? ;" % self._quote_name(primary_keys[-1]))
        parameters.append((primary_keys[-1], item.properties[primary_keys[-1]]))
        
        paramstyle = self.get_db_module().paramstyle
        request, parameters = self._format_request(request.tounicode(),
                                                   parameters, paramstyle)
        
        cursor = connection.cursor()
        cursor.execute(request, parameters)
        connection.commit()
        cursor.close()
    
    def get_connection(self):
        """Return a DB-API connection object and the table name in a tuple.
        
        This method must be overriden.

        This method can use config['url'] to connect and may keep connection in
        cache for later calls.
        
        """
        raise NotImplementedError('Abstract method')
    
    def get_db_module(self):
        """Return a DB-API implementation module.
        
        This method must be overriden.
        
        """
        raise NotImplementedError('Abstract method')
    
    def get_storage_properties(self):
        """Return the list of the storage properties."""
        connection, table = self.get_connection()
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM %s WHERE 0;'
                       % self._quote_name(self._table))
        return [prop[0] for prop in cursor.description]
    
    def get_table_description(self):
        """Return field description as a dictionnary."""
        connection, table = self.get_connection()
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM %s WHERE 0;'
                       % self._quote_name(self._table))
        fields_names = (desc_values[0] for desc_values in cursor.description)
        desc_names = ('type_code', 
                      'display_size',
                      'internal_size', 
                      'precision', 
                      'scale', 
                      'null_ok')
        desc_dicts = [dict(zip(desc_names, desc_values[1:]))
                      for desc_values in cursor.description]
        return dict(zip(fields_names, desc_dicts))
    
    def _storage_search(self, conditions):
        """Return a list of items matching the "conditions".
        
        "conditions" must be a list of tuples (name, function, value), where
        function is in kalamar.utils.operators values.
        
        """
        connection, table = self.get_connection()
        
        # Process conditions
        sql_condition, python_condition = self._process_conditions(conditions)
        # Build request
        request, parameters = self._build_select_request(
                                  sql_condition,
                                  table,
                                  self.get_db_module().paramstyle
                              )
        # Execute request
        cursor = connection.cursor()
        if parameters:
            cursor.execute(request, parameters)
        else:
            cursor.execute(request)
        descriptions = [description[0] for description in cursor.description]
        dict_lines = (dict(zip(descriptions, line))
                      for line in self._generate_lines_from_cursor(cursor))
        
        # Filter result and yield tuples (properties, value)
        filtered = list(self._filter_result(dict_lines, python_condition))
        
        def lines_to_string(lines):
            for line in lines:
                for key in line:
                    if key != self.config['content_column'] \
                    and not isinstance(line[key], unicode):
                        line[key] = str(line[key]).decode(self.default_encoding)
                yield line
        
        for line in lines_to_string(filtered):
            data = line[self.config['content_column']]
            line.pop(self.config['content_column'])
            yield (line, _opener(data))
        
        cursor.close()
    
    def _generate_lines_from_cursor(self, cursor):
        result = cursor.fetchmany()
        while result:
            for line in result:
                yield line
            result = cursor.fetchmany()
    
    @staticmethod
    def _filter_result(dict_lines, conditions):
        """Generate lines matching conditions.
        
        Fixture
        >>> lines = ({'name' : 'toto', 'number' : 42},
        ...          {'name' : 'tata', 'number' : 123},
        ...          {'name' : 'toto', 'number' : 123})
        >>> conditions = (('name', utils.operators['='], 'toto'),
        ...               ('number', utils.operators['='], 42))
        
        Test
        >>> for line in DBAPIStorage._filter_result(lines, conditions):
        ...     print line
        {'name': 'toto', 'number': 42}
        
        """
        for line in dict_lines:
            for name, function, value in conditions:
                if not function(line[name], value):
                    break
            else:
                yield line
    
    def _process_conditions(self, conditions):
        """Return (sql_conditions, python_conditions).
        
        Fixture
        >>> storage = DBAPIStorage(url='toto', basedir='tata')
        >>> conditions = (
        ...     utils.Condition(u'name', utils.operators[u'='], u'toto'),
        ...     utils.Condition(u'number', utils.operators[u'<'], 42),
        ...     utils.Condition(u'number', utils.operators[u'~='], 42),
        ...     utils.Condition(u'number', utils.operators[u'~!='], 42)
        ... )
        
        Test
        >>> sql_condition, python_condition = \
              storage._process_conditions(conditions)
        >>> for c in sql_condition: print c
        Condition(u'name', <built-in function eq>, u'toto')
        Condition(u'number', <built-in function lt>, 42)
        >>> for c in python_condition: print c # doctest:+ELLIPSIS
        Condition(u'number', <function re_match at 0x...>, 42)
        Condition(u'number', <function re_not_match at 0x...>, 42)
        
        """
        sql_conditions = []
        python_conditions = []
        
        for condition in conditions:
            operator_str = utils.operators_rev[condition.operator]
            if operator_str in self.sql_operators:
                operator = utils.operators[self.sql_operators[operator_str]]
                sql_conditions.append(
                    utils.Condition(condition.property_name,
                                    operator, condition.value))
            else:
                python_conditions.append(condition)

        return (sql_conditions, python_conditions)
    
    def _build_select_request(self, conditions, table, style):
        """Return a tuple (request, typed_parameters).
        
        "conditions" must be a sequence of Condition objects.
        "table" is the name of the used table.
        "style" must be one of 'qmark', 'numeric', 'named', 'format',
                  'pyformat'.
        
        The returned "formated_request" is in the DB-API 2 style given by
        paramstyle (see DB-API spec.).

        The returned "typed_parameters" is a list or a dictionnary, according
        to the database parameters style.
        
        Fixture
        >>> conditions = (
        ...     utils.Condition(u"toto", utils.operators[u"="], u"tata"),
        ...     utils.Condition(u"the_answer", utils.operators[u">="], 42)
        ... )
        >>> storage = DBAPIStorage(url='toto', basedir='tata')
        >>> class db_mod:
        ...     BINARY = 1
        ...     DATETIME = 1
        ...     def Binary(self, data):
        ...         return data
        >>> storage.get_db_module = lambda: db_mod()
        >>> storage.get_table_description = lambda: {
        ...     'toto': {'type_code': 1},
        ...     'the_answer': {'type_code': 1}
        ... }
        
        Test
        >>> req, params = storage._build_select_request(conditions,
        ...                                             'table', 'qmark')
        >>> req
        u'SELECT * FROM "table" WHERE "toto"=? AND "the_answer">=? ;'
        >>> params
        (u'tata', 42)
        
        If no condition specified, the 'WHERE' statement must not appear.
        >>> req, params = storage._build_select_request((),
        ...                                             'table', 'qmark')
        >>> req
        u'SELECT * FROM "table" ;'
        >>> params
        ()
        
        """
        conditions = list(conditions)
        table = self._sql_escape_quotes(table)
        table = self._quote_name(table)
        named_condition = (
            self._quote_name(condition.property_name) + 
            self.sql_operators[utils.operators_rev[condition.operator]]
            for condition in conditions
        )
        
        # No need to worry about the final '?' when there is no condition since
        # this case is handled in the following 'if-else'.
        request = u'? AND '.join(named_condition) + '?'
        parameters = [(condition.property_name, condition.value)
                      for condition in conditions]
        
        if parameters:
            request = u"SELECT * FROM %s WHERE %s ;" % (table, request)
        else:
            request = u"SELECT * FROM %s ;" % table

        request, parameters = self._format_request(request, parameters, style)
        return (request, parameters)
    
    def _build_update_request(self, table, item, style):
        """Return update request as a string and parameters as a list.
        
        Fixture
        >>> from kalamar._test.corks import CorkItem
        >>> table = u'table'
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> storage._get_primary_keys = lambda: ['pk1', 'pk2']
        >>> class db_mod:
        ...     BINARY = 1
        ...     DATETIME = 1
        ...     def Binary(self, data):
        ...         return data
        >>> storage.get_db_module = lambda: db_mod()
        >>> storage.get_table_description = lambda: {
        ...     'sto_prop': {'type_code': 1},
        ...     'sto_prop2': {'type_code': 1},
        ...     'pk1': {'type_code': 1},
        ...     'pk2': {'type_code': 1},
        ...     'content_col': {'type_code': 1}
        ... }
        >>> item = CorkItem(storage,
        ...                 storage_properties={'sto_prop': 'sto_val',
        ...                                     'sto_prop2': 'sto_val2',
        ...                                     'pk1': 'pk_val1',
        ...                                     'pk2': 'pk_val2'})
        
        Test
        >>> request, param = storage._build_update_request(table, item, 'qmark')
        >>> request #doctest: +NORMALIZE_WHITESPACE
        u'UPDATE "table" SET
          "sto_prop"=? , "pk2"=? , "pk1"=? , "sto_prop2"=? , "content_col"=?
          WHERE "pk1"=? AND "pk2"=?;'
        >>> param #doctest: +NORMALIZE_WHITESPACE
           ('sto_val', 'pk_val2', 'pk_val1', 'sto_val2', '',
            'pk_val1', 'pk_val2')
        
        """
        
        table = self._sql_escape_quotes(table)
        
        primary_keys = self._get_primary_keys()
        keys = item.properties.storage_properties.keys()
        
        item.properties['_content'] = item.serialize()
        
        if self.config['content_column'] in keys:
            keys.remove(self.config['content_column'])
        
        request = array('u',u"UPDATE %s SET " % self._quote_name(table))
        
        parameters = []
        for key in keys:
            request.extend(u"%s=? , " %
                           self._quote_name(self._sql_escape_quotes(key)))
            parameters.append((key, item.properties[key]))
        data_col = self._quote_name(
                       self._sql_escape_quotes(self.config['content_column'])
                   )
        request.extend(u"%s=? WHERE" % data_col)
                       
        parameters.append((
            self.config['content_column'],
            self.get_db_module().Binary( item.properties['_content'])
        ))
        
        for key in primary_keys[:-1]:
            request.extend(
                u" %s=? AND" % self._quote_name(self._sql_escape_quotes(key))
            )
            parameters.append((key, item.properties[key]))
        request.extend(
            u' %s=?;' % self._quote_name(
                            self._sql_escape_quotes(primary_keys[-1])
                        )
        )
        parameters.append((primary_keys[-1], item.properties[primary_keys[-1]]))
        
        request, parameters = self._format_request(request.tounicode(),
                                                   parameters, style)
        return (request, parameters)
    
    def _build_insert_request(self, table, item, style):
        """Return insert request as a unciode string.
        
        Fixture
        >>> from kalamar._test.corks import CorkItem
        >>> table = 'table'
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> class db_mod:
        ...     BINARY = 1
        ...     DATETIME = 1
        ...     def Binary(self, data):
        ...         return data
        >>> storage.get_db_module = lambda: db_mod()
        >>> storage.get_table_description = lambda: {
        ...     'sto_prop': {'type_code': 1},
        ...     'sto_prop2': {'type_code': 1},
        ...     'pk1': {'type_code': 1},
        ...     'pk2': {'type_code': 1},
        ...     'content_col': {'type_code': 1}
        ... }
        >>> item = CorkItem(storage,
        ...                 storage_properties={'sto_prop': 'sto_val',
        ...                                     'sto_prop2': 'sto_val2',
        ...                                     'pk1': 'pk_val1',
        ...                                     'pk2': 'pk_val2'})
        
        Test
        >>> request, param = storage._build_insert_request(table, item, 'qmark')
        >>> request #doctest:+NORMALIZE_WHITESPACE
        u'INSERT INTO "table"
          ( "sto_prop" , "pk2" , "pk1" , "sto_prop2" , "content_col" )
          VALUES ( ? , ? , ? , ? , ? );'
        >>> param #doctest:+NORMALIZE_WHITESPACE
            ('sto_val', 'pk_val2', 'pk_val1', 'sto_val2', '')

        """
        
        table = self._sql_escape_quotes(table)
        item.properties['_content'] = item.serialize()
        
        request = array('u', u"INSERT INTO %s ( " % self._quote_name(table))
        keys = item.properties.storage_properties.keys()
        if self.config['content_column'] in keys:
            keys.remove(self.config['content_column'])
        
        parameters = []
        
        for key in keys:
            request.extend(u"%s , "
                       % self._quote_name(self._sql_escape_quotes(key)))
        request.extend(u"%s ) VALUES ( "
                   % self._quote_name(self.config['content_column']))
        
        for key in keys:
            request.extend(u"? , ")
            parameters.append((key, item.properties[key]))
        request.extend(u"? );")
        
        parameters.append((
            self.config['content_column'],
            self.get_db_module().Binary(item.properties['_content'])
        ))
        
        request, parameters = self._format_request(request.tounicode(),
                                                   parameters, style)
        return (request, parameters)
    
    def _convert_parameters(self, parameters):
        """Generate converted parameters.
        
        Dummy method meant to be overriden.
        
        """
        return parameters
    
    def _format_request(self, request, parameters, style):
        """Format request and parameters according to "style" and return them.
        
        "parameters" must be ordered according to the request.
        
        Fixture
        >>> request = "DO STHG INTO TABLE ? WHERE toto=? AND tata=?;"
        >>> parameters = [
        ...                  ('toto', 'table'),
        ...                  ('toto', 'toto'),
        ...                  ('toto', 'tata')
        ...              ]
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> class db_mod:
        ...     BINARY = 1
        ...     DATETIME = 1
        ...     def Binary(self, data):
        ...         return data
        >>> storage.get_db_module = lambda: db_mod()
        >>> storage.get_table_description = lambda: {
        ...     'toto': {'type_code': 1}
        ... }
        
        Test
        >>> storage._format_request(request, parameters, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE ? WHERE toto=? AND tata=?;',
         ('table', 'toto', 'tata'))
         
        >>> storage._format_request(request, parameters, 'numeric')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE :1 WHERE toto=:2 AND tata=:3;',
         ('table', 'toto', 'tata'))
         
        >>> storage._format_request(request, parameters, 'named')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE :name0 WHERE toto=:name1 AND tata=:name2;',
        {'toto1': 'toto', 'toto0': 'table', 'toto2': 'tata'})
        
        >>> storage._format_request(request, parameters, 'format')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE %s WHERE toto=%s AND tata=%s;',
         ('table', 'toto', 'tata'))
         
        >>> storage._format_request(request, parameters, 'nonexistant')
        Traceback (most recent call last):
          ...
        UnsupportedParameterStyleError: nonexistant

        
        TODO test 'pyformat'

        """
        
        parameters = list(self._convert_parameters(parameters))
        description = self.get_table_description()
        
        #used with 'format' and 'pyformat'
        format_codes = {
            int: 'i',
            long: 'i',
            str: 's',
            float: 'f',
        }
        
        parts = request.split('?')
        
        if style == 'qmark':
            # ... WHERE a=? AND b=?
            request = u'?'.join(parts)
            parameters = tuple(value for field_name, value in parameters)
        
        elif style == 'numeric':
            # ... WHERE a=:1 AND b=:2
            numbered = ['%s:%i' % (part, i + 1)
                        for i, part in enumerate(parts[:-1])]
            numbered.append(parts[-1])
            request = u''.join(numbered)
            parameters = tuple(value for field_name, value in parameters)
            
        elif style == 'named':
            # ... WHERE a=:name0 AND b=:name1
            named = ['%s:name%i' % (part, i)
                     for i, part in enumerate(parts[:-1])]
            named.append(parts[-1])
            request = u''.join(named)
            parameters = dict(
                             ('%s%i' % (field_name, i), value)
                             for i, (field_name, value)
                             in enumerate(parameters)
                         )
            
        elif style == 'format':
            # ... WHERE a=%s AND b=%d
            request = parts[0]
            for i, part in enumerate(parts[1:]):
                name, value = parameters[i]
                code = format_codes.get(type(value), u's')
                request += u'%%%c%s' % (code, part)
            parameters = tuple(value for field_name, value in parameters)
                
            
        elif style == 'pyformat':
            # ... WHERE name=%(name)s
            raise NotImplementedError # TODO

        else:
            raise DBAPIStorage.UnsupportedParameterStyleError(style)
        
        return (request, parameters)
    
    @staticmethod
    def _sql_escape_quotes(message):
        """Return quote-escaped string.
        
        >>> DBAPIStorage._sql_escape_quotes("'aaaa 'b' jgfoi''")
        "''aaaa ''b'' jgfoi''''"
        
        """
        return message.replace("'", "''")
    
    @staticmethod
    def _quote_name(name):
        """Quote an SQL name (e.g. table name, column name) with the appropriate
        character.
        
        This method use the character " (double-quotes) by default. It should be
        redefined for SGDBs usinf a different character.
        
        """
        return '"%s"' % name
    
    def _get_primary_keys(self):
        """Return a list of primary keys names.

        This method must be overriden.

        """
        raise NotImplementedError('Abstract method')
