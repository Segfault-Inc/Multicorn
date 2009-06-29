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

from kalamar.storage.base import AccessPoint
from kalamar import utils
from kalamar import Item
from array import array
from copy import deepcopy
from itertools import izip

def _opener(content):
    def opener():
        return StringIO(content)
    return opener

class DBAPIStorage(AccessPoint):
    """Base class for SQL SGBD Storage.
    
    Descendant class must override ``get_connection'' and ``_get_primary_keys''.
    
    """
    
    protocol = None
    
    # Provided to ensure compatibility with as many SGDB as possible.
    # May be modified by a descendant.
    sql_operators = {
        '=':   '=',
        '!=':  '!=',
        '>':   '>',
        '>=':  '>=',
        '<':   '<',
        '<=':  '<=',
        }
    
    def __init__(self, **config):
        super(DBAPIStorage, self).__init__(**config)
        self._operators_rev = dict(((b,a) for (a,b) in utils.operators.items()))
    
    def get_connection():
        """Return a DB-API connection object and the table name in a tuple.
        
        This method must be overriden.
        This method can use config['url'] to connect and may keep connection in
        cache for later calls.
        
        """
        raise NotImplementedError('Abstract method')
    
    def get_storage_properties(self):
        connection, table = self.get_connection()
        cur = connection.cursor()
        cur.execute('select * from %s where 0'%self._table)
        return [prop[0] for prop in cur.description]
    
    def _storage_search(self, conditions):
        """Return a list of items matching the ``conditions''.
        
        ``conditions'' must be a list of tuples (name, function, value), where
        function is in kalamar.utils.operators' values.
        
        """
        connection, table = self.get_connection()
        
        # process conditions
        sql_cond, python_cond = self._process_conditions(conditions)
        # build request
        request, parameters = self._build_select_request(sql_cond, table,
                                                         connection.paramstyle)
        # execute request
        cur = connection.cursor()
        cur.execute(request, parameters)
        dict_lines = (dict(zip(cur.description, line))
                      for line in self._gen_lines_from_cursor(cur))
        
        # filter result and yield tuples (properties, value)
        filtered = _filter_result(dict_lines, python_cond)
        cur.close()
        
        return ((line, _opener(line[self.config['content_column']]))
                for line in filtered)
    
    def _gen_lines_from_cursor(cursor):
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
            for name, funct, value in conditions:
                if not funct(line[name],value):
                    break
            else:
                yield line
    
    def _process_conditions(self, conditions):
        """Return (sql_cond, python_cond)
        
        Fixture
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata')
        >>> conditions = (('name', utils.operators['='], 'toto'),
        ...               ('number', utils.operators['<'], 42),
        ...               ('number', utils.operators['~='], 42),
        ...               ('number', utils.operators['~!='], 42))
        
        Test
        >>> sql_cond, python_cond = storage._process_conditions(conditions)
        >>> for c in sql_cond: print c
        ('name', '=', 'toto')
        ('number', '<', 42)
        >>> for c in python_cond: print c # doctest:+ELLIPSIS
        ('number', <function re_match at 0x...>, 42)
        ('number', <function re_not_match at 0x...>, 42)
        
        """
        def cond_with_oper():
            for name, function, value in conditions:
                yield (name, self._operators_rev[function], value)
        
        sql_cond = ((name, self.sql_operators[oper], value)
                        for name, oper, value in cond_with_oper()
                        if (oper in self.sql_operators.keys()))
        python_cond = ((name, utils.operators[oper], value)
                        for name, oper, value in cond_with_oper()
                        if (oper not in self.sql_operators.keys()))
        return (sql_cond, python_cond)
    
    def _build_select_request(self, conditions, table, style):
        """Return a tuple (request, typed_parameters).
        
        ``conditions'' must be a sequence of (name, operator, value).
        ``table'' is the name of the used table.
        ``style'' must be one of 'qmark', 'numeric', 'named', 'format',
                  'pyformat'.
        
        The returned ``formated_request'' is in the DB-API 2 style given by
        paramstyle (see DB-API spec.).
        The returned ``typed_parameters'' is a list or a dictionnary, according
        to the style.
        
        Fixture
        >>> conditions = (("toto", "=", "tata"), ("the_answer", ">=", 42))
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata')
        
        Test
        >>> req, params = storage._build_select_request(conditions,
        ...                                             'table', 'qmark')
        >>> req
        u"SELECT * FROM 'table' WHERE 'toto'=? and 'the_answer'>=? ;"
        >>> params
        ['tata', 42]
        
        """
        table = self._sql_escape_quotes(table)
        named_cond = ("'"+self._sql_escape_quotes(name)+"'" + oper
                      for name, oper, value in conditions)
        
        
        
        request = u'? and '.join(named_cond) + '?'
        parameters = [value for name, oper, value in conditions]
            
        request = u"SELECT * FROM '" + table + "' WHERE " + request + " ;"
        request, parameters = self._format_request(request, parameters,
                                                           style)
        return (request, parameters)
    
    def _build_update_request(self, table, item, style):
        """Return update request as a string
        
        Fixture
        >>> from kalamar._test.corks import CorkItem
        >>> table = 'table'
        >>> item = CorkItem({'sto_prop': 'sto_val',
        ...                  'sto_prop2': 'sto_val2',
        ...                  'pk1': 'pk_val1',
        ...                  'pk2': 'pk_val2'})
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        >>> def monkeypatch(): return ['pk1', 'pk2']
        >>> storage._get_primary_keys = monkeypatch
        
        Test
        >>> storage._build_update_request(table, item, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u"UPDATE 'table' SET 'sto_prop'=? , 'pk2'=? , 'pk1'=? , 'sto_prop2'=?
           WHERE 'pk1'=? 'pk2'=? ;", ['sto_val', 'pk_val2', 'pk_val1',
           'sto_val2', 'pk_val1', 'pk_val2'])
        
        """
        
        table = self._sql_escape_quotes(table)
        
        req = array('u',u"UPDATE '%s' SET "%table)
        pk = self._get_primary_keys()
        keys = item.properties.storage_properties.keys()
        
        item.properties[self.config['content_column']] = item.serialize()
        
        # There is no field '_content' in the DB.
        # save content (item may be used again after being saved)
        content = deepcopy(item.properties['_content'])
        del item.properties['_content']
        
        parameters = []
        
        for key in keys[:-1]:
            req.extend(u"'%s'=? , " % self._sql_escape_quotes(key))
            parameters.append(item.properties[key])
        req.extend(u"'%s'=? WHERE" % self._sql_escape_quotes(keys[-1]))
        parameters.append(item.properties[keys[-1]])
        
        # restore content
        item.properties['_content'] = content
        
        for key in pk:
            req.extend(u" '%s'=?" % self._sql_escape_quotes(key))
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
        >>> item = CorkItem({'sto_prop': 'sto_val',
        ...                  'sto_prop2': 'sto_val2',
        ...                  'pk1': 'pk_val1',
        ...                  'pk2': 'pk_val2'})
        >>> storage = DBAPIStorage(url = 'toto', basedir = 'tata',
        ...                        content_column = 'content_col')
        
        Test
        >>> storage._build_insert_request(table, item, 'qmark')
        ... #doctest:+NORMALIZE_WHITESPACE
        (u"INSERT INTO 'table' ( 'sto_prop' , 'pk2' , 'pk1' , 'sto_prop2' )
           VALUES ( ? , ? , ? , ? );", ['sto_val', 'pk_val2', 'pk_val1',
           'sto_val2'])

        """
        
        table = self._sql_escape_quotes(table)
        
        req = array('u', u"INSERT INTO '%s' ( " % table)
        keys = item.properties.storage_properties.keys()
        
        parameters = []
        
        for key in keys[:-1]:
            req.extend(u"'%s' , " % self._sql_escape_quotes(key))
        req.extend(u"'%s' ) VALUES ( " % self._sql_escape_quotes(keys[-1]))
        
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
        """Format request and parameters according to ``style'' and return them.
        
        ``parameters'' must be ordered according to the request.
        
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
            # ... where a=? and b=?
            request = u'?'.join(parts)
        
        elif style =='numeric':
            # ... where a=:1 and b=:2
            numbered = [part+':'+str(n+1) for n, part in enumerate(parts[:-1])]
            numbered.append(parts[-1])
            request = u''.join(numbered)
            
        elif style == 'named':
            # ... where a=:name0 and b=:name1
            named = [cond+':name'+str(n) for n, cond in enumerate(parts[:-1])]
            named.append(parts[-1])
            request = u''.join(named)
            parameters = (('name'+str(n), param)
                          for n, param
                          in enumerate(parameters))
            parameters = dict(parameters)
            
        elif style == 'format':
            # ... where a=%s and b=%d
            raise NotImplementedError # TODO
            
        elif style == 'pyformat':
            # ... where name=%(name)s
            raise NotImplementedError # TODO
        else:
            raise DBAPIStorage.UnsupportedParameterStyleError(style)
        
        return (request, parameters)
    
    class UnsupportedParameterStyleError(Exception): pass
    
    def save(self, item):
        
        connection, table = self.get_connection()
        
        req = self._build_update_request(table, item)
        
        cursor = connection.cursor()
        cursor.execute(req)
        n = cursor.rowcount()
        if n == 0:
            # item does not exist. Let's do an insert.
            req = self._build_insert_request(table, item)
            cursor.execute(req)
        elif n > 1:
            # problem ocurred
            connection.rollback()
            cursor.close()
            raise ManyItemsUpdatedError()
        # everythings fine
        cursor.commit()
        cursor.close()
    
    class ManyItemsUpdatedError(Exception): pass
    
    @staticmethod
    def _sql_escape_quotes(message):
        """Return quote-escaped string.
        
        >>> DBAPIStorage._sql_escape_quotes("'aaaa 'b' jgfoi''")
        "''aaaa ''b'' jgfoi''''"
        
        """
        return message.replace("'","''")

    def remove(self, item):
        connection, table = self.get_connection()
        req = array('u', 'DELETE FROM %s WHERE ' % table)
        pk = self._get_primary_keys()
        for key in pk[:-1]:
            req.extend('%s = %s AND ' % (key, item.properties[key]))
        req.extend('%s = %s ;' % (pk[-1], item.properties[pk[-1]]))
        
        cursor = connection.cursor()
        cursor.execute(req)
    
    def _get_primary_keys(self):
        """Return a list of primary keys names."""
        raise NotImplementedError('Abstract method')
