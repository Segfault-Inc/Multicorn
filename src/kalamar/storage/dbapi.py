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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

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
        cur = self._connection.cursor()
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
        >>> config = {'url' : '', 'basedir' : ''}
        >>> storage = DBAPIStorage(**config)
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
    
    @staticmethod
    def _build_select_request(conditions, table, style='qmark'):
        """Return a tuple (request, typed_parameters).
        
        ``conditions'' must be a sequence of (name, operator, value).
        ``table'' is the name of the used table.
        ``style'' must be one of 'qmark', 'numeric', 'named', 'format',
                  'pyformat'.
        
        The returned ``formated_request'' is in the DB-API 2 style given by
        paramstyle (see DB-API spec.).
        The returned ``typed_parameters'' is a list or a dictionnary, according
        to the style.
        
        Some test fixture
        >>> conditions = (("toto", "=", "tata"), ("the_answer", ">=", 42))
        
        Qmark style
        >>> req, params = DBAPIStorage._build_select_request(conditions,
        ...                                                  'table', 'qmark')
        >>> req
        u"SELECT * FROM 'table' WHERE 'toto'=? and 'the_answer'>=? ;"
        >>> print params
        ['tata', 42]
        
        Numeric style
        >>> req, params = DBAPIStorage._build_select_request(conditions,
        ...                                                  'table', 'numeric')
        >>> req
        u"SELECT * FROM 'table' WHERE 'toto'=:1 and 'the_answer'>=:2 ;"
        
        Named style
        >>> req, params = DBAPIStorage._build_select_request(conditions,
        ...                                                  'table', 'named')
        >>> req
        u"SELECT * FROM 'table' WHERE 'toto'=:name0 and 'the_answer'>=:name1 ;"
        >>> print params
        {'name0': 'tata', 'name1': 42}
        
        TODO : test 'format' style and 'pyformat' style.
        """
        
        named_cond = ("'"+name+"'" + oper for name, oper, value in conditions)
        if style == 'qmark':
            # ... where a=? and b=?
            request = u'? and '.join(named_cond) + '?'
            parameters = [value for name, oper, value in conditions]
        
        elif style =='numeric':
            # ... where a=:1 and b=:2
            parts = (cond+':'+str(n+1) for n, cond in enumerate(named_cond))
            request = u' and '.join(parts)
            parameters = [value for name, oper, value in conditions]
            
        elif style == 'named':
            # ... where a=:name0 and b=:name1
            parts = (cond+':name'+str(n) for n, cond in enumerate(named_cond))
            request = u' and '.join(parts)
            parameters = (('name'+str(n),value) for n, (name, oper, value)
                          in enumerate(conditions))
            parameters = dict(parameters)
            
        elif style == 'format':
            # ... where a=%s and b=%d
            raise NotImplementedError # TODO
            
        elif style == 'pyformat':
            # ... where name=%(name)s
            raise NotImplementedError # TODO
        else:
            raise UnsupportedParameterStyleError(style)
            
        request = u"SELECT * FROM '" + table + "' WHERE " + request + " ;"
        return (request, parameters)
        
    class UnsupportedParameterStyleError(Exception): pass
    
    def _build_update_request(self, table, item):
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
        >>> storage._build_update_request(table, item)
        ... #doctest:+NORMALIZE_WHITESPACE
        u"UPDATE 'table' SET 'sto_prop' = 'sto_val' , 'pk2' = 'pk_val2' ,
                             'pk1' = 'pk_val1' , 'sto_prop2' = 'sto_val2'
        WHERE 'pk1' = 'pk_val1' 'pk2' = 'pk_val2' ;"
        """
        req = array('u',u"UPDATE '%s' SET "%table)
        pk = self._get_primary_keys()
        keys = item.properties.storage_properties.keys()
        
        item.properties[self.config['content_column']] = item.serialize()
        
        # There is no field '_content' in the DB.
        # save content (item may be used again after being saved)
        content = deepcopy(item.properties['_content'])
        del item.properties['_content']
        
        for key in keys[:-1]:
            req.extend(u"'%s' = '%s' , " % (key, item.properties[key]))
        req.extend(u"'%s' = '%s' WHERE" % (keys[-1], item.properties[keys[-1]]))
        
        # restore content
        item.properties['_content'] = content
        
        for key in pk:
            req.extend(u" '%s' = '%s'" % (key, item.properties[key]))
        req.extend(u' ;')
        
        return req.tounicode()
    
    def save(self, item):
        
        connection, table = self.get_connection()
        
        req = self._build_update_request(table, item)
        
        cursor = connection.cursor()
        cursor.execute(req)
        n = cursor.rowcount()
        if n == 0:
            # item does not exist. Let's do an insert.
            req = array('u', u'INSERT INTO %s ( ' % table)
            for key in keys[:-1]:
                req.extend('%s , ' % key)
            req.extend('%s ) VALUES ( ' % keys[-1])
            
            for key in keys[:-1]:
                req.extend('%s , ' % item.properties[key])
            req.extend('%s );' % item.properties[key[-1]])
            
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
