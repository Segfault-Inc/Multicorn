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

from kalamar.storage.base import AccessPoint
from kalamar import utils
from kalamar import Item
from array import array
from copy import deepcopy
from itertools import izip
from StringIO import StringIO

def _opener(content):
    def opener():
        return StringIO(content)
    return opener

class DBAPIStorage(AccessPoint):
    """Base class for SQL SGBD Storage.
    
    Descendant class must override ``get_connection'', ``_get_primary_keys'',
    ``get_db_module'' and "protocol".
    It may be useful to also redefine the following methods or attributes:
        - _quote_name
        - _sql_escape_quotes
        - sql_operators
    
    """
    protocol = None
    
    class UnsupportedParameterStyleError(Exception): pass
    class ManyItemsUpdatedError(Exception): pass
    
    # Provided to ensure compatibility with as many SGDB as possible.
    # May be modified by a descendant.
    sql_operators = {
        '=': '=',
        '!=': '!=',
        '>': '>',
        '>=': '>=',
        '<': '<',
        '<=': '<=',
        }
    
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
        cursor.execute('SELECT * FROM %s WHERE 0' % self._quote_name(self._table))
        return [prop[0] for prop in cursor.description]
    
    def _storage_search(self, conditions):
        """Return a list of items matching the "conditions".
        
        "conditions" must be a list of tuples (name, function, value), where
        function is in kalamar.utils.operators values.
        
        """
        connection, table = self.get_connection()
        
        # process conditions
        sql_cond, python_cond = self._process_conditions(conditions)
        # build request
        paramstyle = self.get_db_module().paramstyle
        request, parameters = self._build_select_request(sql_cond, table,
                                                         paramstyle)
        # execute request
        cur = connection.cursor()
        cur.execute(request, parameters)
        desc = [col_desc[0] for col_desc in cur.description]
        dict_lines = (dict(zip(desc, line))
                      for line in self._gen_lines_from_cursor(cur))
        
        # filter result and yield tuples (properties, value)
        filtered = list(self._filter_result(dict_lines, python_cond))
        
        for line in filtered:
            yield (line, _opener(line[self.config['content_column']]))
        
        cur.close()
    
    def _gen_lines_from_cursor(self, cursor):
        res = cursor.fetchmany()
        while res:
            for line in res:
                yield line
            res = cursor.fetchmany()
    
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
        >>> for l in DBAPIStorage._filter_result(lines, conditions):
        ...     print l
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
        
        for cond in conditions:
            op_str = utils.operators_rev[cond.operator]
            if op_str in self.sql_operators:
                sql_conditions.append(utils.Condition(cond.property_name,
                    utils.operators[self.sql_operators[op_str]], cond.value))
            else:
                python_conditions.append(cond)

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
        to the style.
        
        Fixture
        >>> conditions = (
        ...     utils.Condition(u"toto", utils.operators[u"="], u"tata"),
        ...     utils.Condition(u"the_answer", utils.operators[u">="], 42)
        ... )
        >>> storage = DBAPIStorage(url='toto', basedir='tata')
        
        Test
        >>> req, params = storage._build_select_request(conditions,
        ...                                             'table', 'qmark')
        >>> req
        u'SELECT * FROM "table" WHERE "toto"=? and "the_answer">=? ;'
        >>> params
        [u'tata', 42]
        
        If no condition specified, the 'WHERE' statement must not appear.
        >>> req, params = storage._build_select_request((),
        ...                                             'table', 'qmark')
        >>> req
        u'SELECT * FROM "table" ;'
        >>> params
        []
        
        """
        conditions = list(conditions)
        table = self._sql_escape_quotes(table)
        table = self._quote_name(table)
        named_cond = (self._quote_name(cond.property_name) + 
                      self.sql_operators[utils.operators_rev[cond.operator]]
                      for cond in conditions)
        
        
        
        # No need to worry about the final '?' when there is no condition since
        # this case is handled in the following 'if-else'.
        request = u'? and '.join(named_cond) + '?'
        parameters = [cond.value for cond in conditions]
        
        if len(parameters) == 0:
            request = u"SELECT * FROM " + table + " ;"
        else:
            request = u"SELECT * FROM " + table + " WHERE " + request + " ;"
        request, parameters = self._format_request(request, parameters,
                                                           style)
        return (request, parameters)
    
    def _build_update_request(self, table, item, style):
        """Return update request as a string.
        
        Fixture
        >>> from kalamar._test.corks import CorkItem
        >>> table = 'table'
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> item = CorkItem(storage,
        ...                 storage_properties={'sto_prop': 'sto_val',
        ...                                     'sto_prop2': 'sto_val2',
        ...                                     'pk1': 'pk_val1',
        ...                                     'pk2': 'pk_val2'})
        >>> def monkeypatch(): return ['pk1', 'pk2']
        >>> storage._get_primary_keys = monkeypatch
        
        Test
        >>> storage._build_update_request(table, item, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'UPDATE "table" SET "sto_prop"=? , "pk2"=? , "pk1"=? , "sto_prop2"=?
           WHERE "pk1"=? "pk2"=? ;', ['sto_val', 'pk_val2', 'pk_val1',
           'sto_val2', 'pk_val1', 'pk_val2'])
        
        """
        
        table = self._sql_escape_quotes(table)
        
        req = array('u',u"UPDATE %s SET " % self._quote_name(table))
        pk = self._get_primary_keys()
        keys = item.properties.keys_without_aliases()
        
        item.properties[self.config['content_column']] = item.serialize()
        
        # There is no field '_content' in the DB
        if '_content' in keys:
            # Save content (item may be used again after being saved)...
            content = deepcopy(item.properties['_content'])
            # then delete it in "keys".
            keys.remove('_content')
        
        parameters = []
        for key in keys[:-1]:
            req.extend(u"%s=? , "
                       % self._quote_name(self._sql_escape_quotes(key)))
            parameters.append(item.properties[key])
        req.extend(u"%s=? WHERE"
                   % self._quote_name(self._sql_escape_quotes(keys[-1])))
        parameters.append(item.properties[keys[-1]])
        
        for key in pk:
            req.extend(u" %s=?"
                       % self._quote_name(self._sql_escape_quotes(key)))
            parameters.append(item.properties[key])
        req.extend(u' ;')
        
        request, parameters = self._format_request(req.tounicode(), parameters,
                                                   style)
        return (request, parameters)
    
    def _build_insert_request(self, table, item, style):
        """Return insert request as a unciode string.
        
        Fixture
        >>> from kalamar._test.corks import CorkItem
        >>> table = 'table'
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> item = CorkItem(storage,
        ...                 storage_properties={'sto_prop': 'sto_val',
        ...                                     'sto_prop2': 'sto_val2',
        ...                                     'pk1': 'pk_val1',
        ...                                     'pk2': 'pk_val2'})
        
        Test
        >>> storage._build_insert_request(table, item, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'INSERT INTO "table" ( "sto_prop" , "pk2" , "pk1" , "sto_prop2" )
           VALUES ( ? , ? , ? , ? );', ['sto_val', 'pk_val2', 'pk_val1',
           'sto_val2'])

        """
        
        table = self._sql_escape_quotes(table)
        
        req = array('u', u"INSERT INTO %s ( " % self._quote_name(table))
        keys = item.properties.storage_properties.keys()
        
        parameters = []
        
        for key in keys[:-1]:
            req.extend(u"%s , "
                       % self._quote_name(self._sql_escape_quotes(key)))
        req.extend(u"%s ) VALUES ( "
                   % self._quote_name(self._sql_escape_quotes(keys[-1])))
        
        for key in keys[:-1]:
            req.extend(u"? , ")
            parameters.append(item.properties[key])
        req.extend(u"? );")
        parameters.append(item.properties[keys[-1]])
        
        request, parameters = self._format_request(req.tounicode(), parameters,
                                                   style)
        return (request, parameters)
    
    @staticmethod
    def _format_request(request, parameters, style):
        """Format request and parameters according to "style" and return them.
        
        "parameters" must be ordered according to the request.
        
        Fixture
        >>> request = "DO STHG INTO TABLE ? WHERE toto=? AND tata=?;"
        >>> parameters = ['table', 'toto', 'tata']
        
        Test
        >>> DBAPIStorage._format_request(request, parameters, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE ? WHERE toto=? AND tata=?;',
         ['table', 'toto', 'tata'])
         
        >>> DBAPIStorage._format_request(request, parameters, 'numeric')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE :1 WHERE toto=:2 AND tata=:3;',
         ['table', 'toto', 'tata'])
         
        >>> DBAPIStorage._format_request(request, parameters, 'named')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u'DO STHG INTO TABLE :name0 WHERE toto=:name1 AND tata=:name2;',
        {'name2': 'tata', 'name0': 'table', 'name1': 'toto'})
        
        TODO test 'format' and 'pyformat'

        """
        
        parts = request.split('?')
        
        if style == 'qmark':
            # ... WHERE a=? AND b=?
            request = u'?'.join(parts)
        
        elif style == 'numeric':
            # ... WHERE a=:1 AND b=:2
            numbered = [part+':'+str(n+1) for n, part in enumerate(parts[:-1])]
            numbered.append(parts[-1])
            request = u''.join(numbered)
            
        elif style == 'named':
            # ... WHERE a=:name0 AND b=:name1
            named = ['%s:name%i' % (condition, n)
                     for n, condition in enumerate(parts[:-1])]
            named.append(parts[-1])
            request = u''.join(named)
            parameters = dict(('name'+str(n), param)
                              for n, param in enumerate(parameters))
            
        elif style == 'format':
            # ... WHERE a=%s AND b=%d
            raise NotImplementedError # TODO
            
        elif style == 'pyformat':
            # ... WHERE name=%(name)s
            raise NotImplementedError # TODO

        else:
            raise DBAPIStorage.UnsupportedParameterStyleError(style)
        
        return (request, parameters)
    
    
    def save(self, item):
        """Save item in the database."""
        connection, table = self.get_connection()
        
        style = self.get_db_module().paramstyle
        
        request, parameters = self._build_update_request(table, item, style)
        
        cursor = connection.cursor()
        cursor.execute(request, parameters)
        n = cursor.rowcount

        if n == 0:
            # Item does not exist, let's do an insert
            request, parameters = self._build_insert_request(table, item, style)
            cursor.execute(request, parameters)
        elif n > 1:
            # Problem ocurred
            connection.rollback()
            cursor.close()
            raise ManyItemsUpdatedError()
        # everything is fine
        connection.commit()
        cursor.close()
    
    @staticmethod
    def _sql_escape_quotes(message):
        """Return quote-escaped string.
        
        >>> DBAPIStorage._sql_escape_quotes("'aaaa 'b' jgfoi''")
        "''aaaa ''b'' jgfoi''''"
        
        """
        return message.replace("'","''")
    
    @staticmethod
    def _quote_name(name):
        """Quote an SQL name (e.g. table name, column name) with the appropriate
        character.
        
        This method use the character " (double-quotes) by default. It should be
        redefined for SGDBs usinf a different character.
        
        """
        return '"'+name+'"'

    def remove(self, item):
        
        connection, table = self.get_connection()
        table = self._sql_escape_quotes(table)
        req = array('u', u"DELETE FROM %s WHERE " % self._quote_name(table))
        
        pk = self._get_primary_keys()
        parameters = []
        
        for key in pk[:-1]:
            req.extend("%s=? AND " % self._quote_name(key))
            parameters.append(item.properties[key])
        req.extend("%s=? ;" % self._quote_name(pk[-1]))
        parameters.append(item.properties[pk[-1]])
        
        paramstyle = self.get_db_module().paramstyle
        request, parameters = self._format_request(req.tounicode(), parameters,
                                                   paramstyle)
        
        cursor = connection.cursor()
        cursor.execute(request, parameters)
        connection.commit()
        cursor.close()
    
    def _get_primary_keys(self):
        """Return a list of primary keys names.

        This method must be overriden.

        """
        raise NotImplementedError('Abstract method')
