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
import thread

from kalamar.storage.base import AccessPoint
from kalamar import utils



def _opener(content):
    #convert to str as some databases return buffer objects
    return lambda: str(content)
    


class Parameter:
    def __init__(self, name, value):
        self.name = name
        self.value = value



class DBAPIStorage(AccessPoint):
    """Base class for SQL SGBD Storage.
    
    Descendant class must override ``get_connection``, ``primary_keys``,
    ``get_db_module`` and ``protocol``.

    It may be useful to also redefine the following methods or attributes:

    - ``_quote_name``
    - ``_sql_escape_quotes``
    - ``sql_operators``
    
    """
    protocol = None
    
    # Provided to ensure compatibility with as many SGDB as possible.
    # This may be modified by a descendant.
    sql_operators = {
        '=': '=',
        '!=': '!=',
        '>': '>',
        '>=': '>=',
        '<': '<',
        '<=': '<='}

    class UnsupportedParameterStyleError(Exception):
        """Unavailable style for parameter."""
    
    def __init__(self, *args, **kwargs):
        super(DBAPIStorage, self).__init__(*args, **kwargs)
        self.content_column = self.config.get('content_column', None)
        self._connections = {}
    
    def save(self, item):
        """Save item in the database."""
        connection, table = self.get_connection()
        
        style = self.get_db_module().paramstyle
        
        request, parameters = self._build_update_request(table, item, style)
        
        cursor = connection.cursor()
        try:
            cursor.execute(request, parameters)
            
            # TODO: this is incorrect: someone might have inserted a row just
            # now.  Instead, we should have uniqueness constaints in the
            # database, try an insert firt, and do an update if the database
            # throws a unique constraint violation.
            # See http://www.databasesandlife.com/unique-constraints/
            if cursor.rowcount == 0:
                # Item does not exist, let's do an insert
                request, parameters = \
                    self._build_insert_request(table, item, style)
                #1/0
                cursor.execute(request, parameters)
            else:
                assert cursor.rowcount == 1

            connection.commit()
        finally:
            cursor.close()

        item.serialize()

    def remove(self, item):
        """Remove the item from the database."""
        connection, table = self.get_connection()
        table = self._sql_escape_quotes(table)
        request = array('u', u"DELETE FROM %s WHERE " % self._quote_name(table))
        
        parameters = []
        
        parameters.extend(Parameter(key, item[key])
                          for key in self.primary_keys)
        request.extend(" AND ".join("%s=?" % self._quote_name(key)
                                    for key in self.primary_keys))
        
        paramstyle = self.get_db_module().paramstyle
        request, parameters = self._format_request(request.tounicode(),
                                                   parameters, paramstyle)
        
        cursor = connection.cursor()
        cursor.execute(request, parameters)
        connection.commit()
        cursor.close()
    
    def get_connection(self):
        """Return a DB-API connection object and the table name in a tuple.
        
        If connection is broken, get_connection will try to get a new one.

        This method can use config['url'] to connect and may keep connection in
        cache for later calls.
        
        """
        if self.get_db_module().threadsafety >= 2:
            # threads can share connections
            key = None
        else:
            # one connection per thread
            key = thread.get_ident()
        try:
            return self._connections[key]
        except KeyError:
            connection, table = self._get_connection()
            self._connections[key] = connection, table
            if hasattr(connection, 'ping'):
                # In MySQLdb: check if the connection is alive and try to
                # reconnect if it's not
                connection.ping(True)
            return connection, table
            
    
    def _get_connection(self):
        """The actual, database-dependent implementation of get_connection.
        
        This method must be overriden by contrete subclasses.
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
        request = 'SELECT * FROM %s WHERE 1=2;' % self._quote_name(table)
        cursor.execute(request)
        properties_names = [prop[0] for prop in cursor.description]
        
        # If a content_column has been declared in kalamar configuration, it
        # becomes an item property (i.e. '_content'), so we remove it from
        # storage properties
        if self.content_column is not None:
            properties_names.remove(self.content_column)
        return properties_names
    
    def get_table_description(self):
        """Return field description as a dictionnary."""
        connection, table = self.get_connection()
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM %s WHERE 1=0;'
                       % self._quote_name(table))
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
        """Return a list of items matching the ``conditions``.
        
        ``conditions`` must be a list of tuples (name, function, value), where
        function is in kalamar.utils.operators values.
        
        """
        connection, table = self.get_connection()
        
        # Process conditions
        sql_condition, python_condition = self._process_conditions(conditions)
        
        # Build request
        request, parameters = self._build_select_request(
            sql_condition, table, self.get_db_module().paramstyle)
        
        # Execute request
        cursor = connection.cursor()
        cursor.execute(request, parameters)
        
        # Release lock on the table we used.
        connection.commit()
    
        descriptions = [description[0] for description in cursor.description]
        dict_lines = (dict(zip(descriptions, line))
                        for line in self._generate_lines_from_cursor(cursor))
        
        # Filter result and yield tuples (properties, value)
        filtered = list(self._filter_result(dict_lines, python_condition))
        for line in filtered:
            data = line.get(self.content_column, None)
            
            # If a content_column has been declared in kalamar configuration, it
            # becomes an item property (i.e. '_content'), so we remove it from
            # storage properties
            if self.content_column is not None:
                line.pop(self.content_column)
                
            yield (line, _opener(data))

        cursor.close()
    
    @staticmethod
    def _generate_lines_from_cursor(cursor):
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
        >>> conditions = (utils.Condition('name', utils.operators['='], 'toto'),
        ...               utils.Condition('number', utils.operators['='], 42))
        
        Test
        >>> for line in DBAPIStorage._filter_result(lines, conditions):
        ...     print line
        {'name': 'toto', 'number': 42}
        
        """
        for line in dict_lines:
            for condition in conditions:
                if not condition.operator(
                    line[condition.property_name], condition.value):
                    break
            else:
                yield line
    
    def _process_conditions(self, conditions):
        """Return ``sql_conditions``, ``python_conditions``.
        
        Fixture
        >>> storage = DBAPIStorage(url='toto', basedir='tata')
        >>> conditions = (
        ...     utils.Condition(u'name', utils.operators[u'='], u'toto'),
        ...     utils.Condition(u'number', utils.operators[u'<'], 42),
        ...     utils.Condition(u'number', utils.operators[u'~='], 42),
        ...     utils.Condition(u'number', utils.operators[u'~!='], 42))
        
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
            
            if operator_str in self.sql_operators and \
               not(condition.value is None and operator_str in ('=', '!=')):
                # Operators other than = and != have a weird behaviour in python
                # wich differs from SQL.
                operator = utils.operators[self.sql_operators[operator_str]]
                sql_conditions.append(
                    utils.Condition(condition.property_name,
                                    operator, condition.value))
            else:
                python_conditions.append(condition)

        return sql_conditions, python_conditions
    
    def _build_select_request(self, conditions, table, style):
        """Return a tuple (request, typed_parameters).
        
        "conditions`` must be a sequence of Condition objects.
        ``table`` is the name of the used table.
        ``style`` must be one of 'qmark', 'numeric', 'named', 'format',
                  'pyformat'.
        
        The returned ``formated_request`` is in the DB-API 2 style given by
        paramstyle (see DB-API spec.).

        The returned ``typed_parameters`` is a list or a dictionnary, according
        to the database parameters style.
        
        Fixture
        >>> conditions = (
        ...     utils.Condition(u'toto', utils.operators[u'='], u'tata'),
        ...     utils.Condition(u'the_answer', utils.operators[u'>='], 42))
        >>> storage = DBAPIStorage(url='toto', basedir='tata',
        ...                        content_column = 'content_col')
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
        >>> req, params = storage._build_select_request((), 'table', 'qmark')
        >>> req
        u'SELECT * FROM "table" ;'
        >>> params
        ()

        If a condition value is None, replace the test by IS (NOT) NULL
        >>> conditions = (utils.Condition(u'toto', utils.operators[u'='], None),
        ...     utils.Condition(u'the_answer', utils.operators[u'!='], None))
        >>> req, params = storage._build_select_request(conditions,
        ...                                             'table', 'qmark')
        >>> req
        u'SELECT * FROM "table" WHERE "toto" IS NULL AND "the_answer" IS NOT NULL ;'
        >>> params
        ()

        """
        conditions = list(conditions)
        table = self._sql_escape_quotes(table)
        table = self._quote_name(table)
        
        def named_conditions(conditions):
            for condition in conditions:
                operator = utils.operators_rev[condition.operator]
                if condition.value is None:
                    if operator == '=':
                        yield self._quote_name(condition.property_name) + \
                              ' IS NULL'
                    elif operator == '!=':
                        yield self._quote_name(condition.property_name) + \
                              ' IS NOT NULL'
                    # else: nothing to do since others operators are handled in
                    # python for None properties (see _process_conditions).
                else:
                    yield self._quote_name(condition.property_name) + \
                          self.sql_operators[operator] + '?'
        
        request = u' AND '.join(named_conditions(conditions))
        
        # We don't use conditions because others _build_***_request don't have
        # access to them.
        parameters = [
            Parameter(condition.property_name,condition.value)
            for condition in conditions
            if condition.value is not None]
        
        if conditions:
            request = u'SELECT * FROM %s WHERE %s ;' % (table, request)
        else:
            request = u'SELECT * FROM %s ;' % table

        request, parameters = self._format_request(request, parameters, style)
        return request, parameters
    
    def _build_update_request(self, table, item, style):
        """Return update request as a string and parameters as a list.
        
        Fixture
        >>> from kalamar._test.corks import CorkItem, CorkDBAPIStorage
        >>> table = 'table'
        >>> storage = CorkDBAPIStorage(url = 'toto', basedir = 'tata',
        ...                            content_column = 'content_col')
        >>> item = CorkItem(storage,
        ...                 storage_properties={'sto_prop': 'sto_val',
        ...                                     'sto_prop2': 'sto_val2',
        ...                                     'pk1': 'pk_val1',
        ...                                     'pk2': 'pk_val2'})
        
        Test
        >>> request, param = storage._build_update_request(table, item, 'qmark')
        >>> request #doctest: +NORMALIZE_WHITESPACE
        u'UPDATE "table" SET
          "sto_prop"=? , "pk2"=? , "pk1"=? , "content_col"=? , "sto_prop2"=?
          WHERE "pk1"=? AND "pk2"=? ;'
        >>> param #doctest: +NORMALIZE_WHITESPACE
           ('sto_val', 'pk_val2', 'pk_val1', "item's raw data", 'sto_val2',
            'pk_val1', 'pk_val2')
        
        """
        table = self._sql_escape_quotes(table)
        
        keys = item.raw_storage_properties.keys()
        
        # All not primary keys and all primary keys not None.
        keys = [
            key for key in keys if 
            key not in self.primary_keys or item[key] is not None]

        request = array('u', u'UPDATE %s SET ' % self._quote_name(table))
        
        parameters = []
        
        for key in keys[:-1]:
            request.extend(u'%s=? , ' %
                           self._quote_name(self._sql_escape_quotes(key)))
            parameters.append(Parameter(key, item[key]))
            
        # If a content_column has been declared in kalamar configuration, it
        # becomes an item property (i.e. '_content'), so it is not in the 'keys'
        # list.
        if self.content_column is not None:
            content = item.serialize()
            colname = self._quote_name(
                self._sql_escape_quotes(self.content_column))
            request.extend(u'%s=? , ' % colname)
            parameters.append(
                Parameter(self.content_column, content))
            
        request.extend(u'%s=? WHERE' %
                       self._quote_name(self._sql_escape_quotes(keys[-1])))
        parameters.append(Parameter(keys[-1], item[keys[-1]]))
        
        for key in self.primary_keys[:-1]:
            request.extend(
                u' %s=? AND' % self._quote_name(self._sql_escape_quotes(key)))
            parameters.append(Parameter(key, item[key]))
        request.extend(
            u' %s=? ;' % self._quote_name(
                self._sql_escape_quotes(self.primary_keys[-1])))
        parameters.append(
            Parameter(self.primary_keys[-1], item[self.primary_keys[-1]]))
        
        request, parameters = self._format_request(request.tounicode(),
                                                   parameters, style)
        return request, parameters
    
    def _build_insert_request(self, table, item, style):
        """Return insert request as a unciode string.
        
        Fixture
        >>> from kalamar._test.corks import CorkItem, CorkDBAPIStorage
        >>> table = 'table'
        >>> storage = CorkDBAPIStorage(url = 'toto', basedir = 'tata',
        ...                            content_column = 'content_col')
        >>> item = CorkItem(storage,
        ...                 storage_properties={'sto_prop': 'sto_val',
        ...                                     'sto_prop2': 'sto_val2',
        ...                                     'pk1': 'pk_val1',
        ...                                     'pk2': 'pk_val2'})
        
        Test
        >>> request, param = storage._build_insert_request(table, item, 'qmark')
        >>> request #doctest:+NORMALIZE_WHITESPACE
        u'INSERT INTO "table"
          ( "sto_prop" , "pk2" , "pk1" , "content_col" , "sto_prop2" )
          VALUES ( ? , ? , ? , ? , ? ) ;'
        >>> param #doctest:+NORMALIZE_WHITESPACE
            ('sto_val', 'pk_val2', 'pk_val1', "item's raw data", 'sto_val2')

        """
        table = self._sql_escape_quotes(table)
        
        request = array('u', u'INSERT INTO %s ( ' % self._quote_name(table))
        
        keys = item.raw_storage_properties.keys()
        keys = [
            key for key in keys if 
            key not in self.primary_keys or item[key] is not None]
        
        parameters = []
        
        for key in keys[:-1]:
            request.extend(u'%s , '
                       % self._quote_name(self._sql_escape_quotes(key)))
            
        # If a content_column has been declared in kalamar configuration, it
        # becomes an item property (i.e. '_content'), so it is not in the 'keys'
        # list.
        if self.content_column is not None:
            colname = self._quote_name(
                self._sql_escape_quotes(self.content_column))
            request.extend(u'%s , ' % colname)
        
        request.extend(
            u'%s ) VALUES ( '
            % self._quote_name(self._sql_escape_quotes(keys[-1])))
        
        for key in keys[:-1]:
            request.extend(u'? , ')
            parameters.append(Parameter(key, item[key]))
        
        if self.content_column is not None:
            content = item.serialize()
            request.extend(u'? , ')
            parameters.append(
                Parameter(self.content_column, content))
        
        request.extend(u'? ) ;')
        parameters.append(Parameter(keys[-1], item[keys[-1]]))
        
        request, parameters = self._format_request(request.tounicode(),
                                                   parameters, style)
        return request, parameters
    
    def _convert_parameters(self, parameters):
        """Converts parameters into corresponding field's type.
        
        Dummy method meant to be overriden.
        
        """
        for parameter in parameters:
            if parameter.name == self.content_column:
                parameter.value = self.get_db_module().Binary(parameter.value)
            yield parameter
    
    def _format_request(self, request, parameters, style):
        """Format request and parameters according to ``style`` and return them.
        
        ``parameters`` must be ordered according to the request.
        
        Fixture
        >>> request = "DO STHG INTO TABLE ? WHERE toto=? AND tata=? ;"
        >>> parameters = [Parameter('name', 'table'),
        ...               Parameter('other_name', 1),
        ...               Parameter('yet_a_name', None)]
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> class db_mod:
        ...     BINARY = 1
        ...     DATETIME = 1
        ...     def Binary(self, data):
        ...         return data
        >>> storage.get_db_module = lambda: db_mod()
        >>> storage.get_table_description = lambda: {
        ...     'toto': {'type_code': 1}}
        
        Test
        >>> storage._format_request(request, parameters, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE ? WHERE toto=? AND tata IS NULL ;',
         ('table', 1))
         
        >>> storage._format_request(request, parameters, 'numeric')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE :1 WHERE toto=:2 AND tata IS NULL ;',
         ('table', 1))
         
        >>> storage._format_request(request, parameters, 'named')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE :name0 WHERE toto=:other_name1 AND tata IS NULL ;',
        {u'other_name1': 1, u'name0': 'table'})
        
        >>> storage._format_request(request, parameters, 'format')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE %s WHERE toto=%s AND tata IS NULL ;',
         ('table', 1))
         
        >>> storage._format_request(request, parameters, 'pyformat')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE %(name0)s WHERE toto=%(other_name1)s AND tata IS NULL ;',
        {u'other_name1': 1, u'name0': 'table'})
         
        >>> storage._format_request(request, parameters, 'nonexistant')
        Traceback (most recent call last):
          ...
        UnsupportedParameterStyleError: nonexistant

        """
        # Change '=?' by ' IS NULL' when parameter value is ``None``
        parts = request.split('?')
        request = ''
        for part, parameter in zip(parts, parameters):
            if parameter.value is None:
                request += '%s IS NULL' % part.rstrip('<>=')
            else:
                request += '%s?' % part
        request += parts[-1]
        parameters = [parameter for parameter in parameters
                      if parameter.value is not None]

        # Update request according to ``style``
        parameters = list(self._convert_parameters(parameters))
        parts = request.split('?')
        
        if style == 'qmark':
            # ... WHERE a=? AND b=?
            request = u'?'.join(parts)
            parameters = tuple(p.value for p in parameters)
        
        elif style == 'numeric':
            # ... WHERE a=:1 AND b=:2
            numbered = ['%s:%i' % (part, i + 1)
                        for i, part in enumerate(parts[:-1])]
            numbered.append(parts[-1])
            request = u''.join(numbered)
            parameters = tuple(p.value for p in parameters)
            
        elif style == 'named':
            # ... WHERE a=:name0 AND b=:name1
            named = [
                '%s:%s%i' % (part, param.name, i)
                for i, (part, param)
                in enumerate(zip(parts[:-1], parameters))]
            named.append(parts[-1])
            request = u''.join(named)
            parameters = dict(
                (u'%s%i' % (p.name, i), p.value)
                for i, p in enumerate(parameters))
            
        elif style == 'format':
            # ... WHERE a=%s AND b=%d
            request = parts[0]
            for i, part in enumerate(parts[1:]):
                code = u's'
                request += u'%%%c%s' % (code, part)
            parameters = tuple(p.value for p in parameters)
                
            
        elif style == 'pyformat':
            # ... WHERE a=%(name0)s AND a=%(name1)s
            request = parts[0]
            for i, (part, param) in enumerate(zip(parts[1:], parameters)):
                code = u's'
                request += u'%%(%s%i)%c%s' % (param.name, i, code, part)
            parameters = dict((u'%s%i' % (p.name, i), p.value)
                              for i, p in enumerate(parameters))

        else:
            raise DBAPIStorage.UnsupportedParameterStyleError(style)

        return request, parameters
    
    @staticmethod
    def _sql_escape_quotes(message):
        """Return quote-escaped string.
        
        >>> DBAPIStorage._sql_escape_quotes("'aaaa 'b' jgfoi''")
        "''aaaa ''b'' jgfoi''''"
        
        >>> DBAPIStorage._sql_escape_quotes("teaurt euia etnau")
        'teaurt euia etnau'
        
        """
        return message.replace("'", "''")
    
    @staticmethod
    def _quote_name(name):
        """Quote an SQL ``name`` (e.g. column name) with the appropriate char.
        
        This method use the character " (double-quotes) by default. It should be
        redefined for SGDBs using a different character.
        
        """
        return u'"%s"' % name
