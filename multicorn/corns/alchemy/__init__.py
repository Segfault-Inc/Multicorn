# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from ..abstract import AbstractCorn
from ...requests.types import Type, List, Dict
from ...requests.helpers import cut_on_predicate
from ... import python_executor
from multicorn.utils import print_sql

class InvalidRequestException(Exception):

    def __init__(self, request, message=""):
        self.request = request
        self.message = message


try:
    import sqlalchemy
except ImportError:
    import sys
    print("WARNING: The SQLAlchemy AP is not available.", file=sys.stderr)
else:
    from sqlalchemy import create_engine, Table, Column, MetaData
    from sqlalchemy import sql as sqlexpr

DEFAULT_VALUE = object()

class ColumnDefinition(object):

    def __init__(self, type, db_gen=False, column_name=None):
        self.type = type
        self.db_gen = db_gen
        self.column_name = column_name


class Alchemy(AbstractCorn):

    def __init__(self, name, identity_properties=None, url="sqlite:///",
            tablename=None, schema=None, engine_opts=None, create_table=True):
        super(Alchemy, self).__init__(name, identity_properties)
        self.__table = None
        self.url = url
        self.__open_statement = None
        self.__insert_statement = None
        self.__update_statement = None
        self.tablename = tablename or name.lower()
        self.schema = schema
        self.engine_opts = engine_opts or {}
        self.create_table = True
        self.definitions = {}

    def bind(self, multicorn):
        super(Alchemy, self).bind(multicorn)
        if not hasattr(self.multicorn, '_alchemy_metadatas'):
            self.multicorn._alchemy_metadatas = {}
        metadata = self.multicorn._alchemy_metadatas.get(self.url, None)
        if metadata is None:
            engine = create_engine(self.url, **self.engine_opts)
            metadata = MetaData()
            metadata.bind = engine
            self.multicorn._alchemy_metadatas[self.url] = metadata
        self.metadata = metadata
        # TODO: manage dialect creation here
        self.dialect = get_dialect(metadata.bind)

    def create(self, props=None, lazy_props=None):
        props = props or {}
        lazy_props = lazy_props or {}
        for name in self._generated_keys:
            if name not in props and name not in lazy_props:
                props[name] = DEFAULT_VALUE
        return super(Alchemy, self).create(props, lazy_props)

    def register(self, name, type=unicode, db_gen=None, column_name=None):
        if db_gen is None:
            if name in self.identity_properties:
                db_gen = True
            else:
                db_gen = False
        column_name = column_name or name
        type = Type(corn=self, name=name, type=type)
        self.properties[name] = type
        self.definitions[name] = ColumnDefinition(type, db_gen=db_gen, column_name=column_name)

    @property
    def table(self):
        if self.__table is not None:
            return self.__table
        columns = []
        for name, prop in sorted(self.definitions.iteritems()):
            kwargs = {}
            if name in self.identity_properties:
                kwargs["primary_key"] = True
            type = self.dialect.alchemy_type(prop.type)
            column = Column(prop.column_name, type, key=name, **kwargs)
            columns.append(column)
        kwargs = dict(useexisting=True)
        if self.schema:
            kwargs['schema'] = self.schema
        table = Table(self.tablename, self.metadata, *columns,
                **kwargs)
        if self.create_table == True:
            table.create(checkfirst=True)
        self.__table = table
        return table

    def _all(self):
        return self.dialect._transform_result(self.table.select().execute(), List(self.type), self)

    def _to_pk_where_clause(self, item):
        conditions = []
        for key in self.identity_properties:
             conditions.append(self.table.columns[key] == item[key])
        return sqlexpr.and_(*conditions)

    @property
    def open_statement(self):
        if not self.__open_statement:
            conditions = []
            for key in self.identity_properties:
                 # Prefix with "b_" like adviced from sqlalchemy because
                 # of reserved names
                 conditions.append(self.table.columns[key] == sqlexpr.bindparam("b_%s" % key))
            statement = self.table.select().where(sqlexpr.and_(*conditions))
            self.__open_statement = statement.compile()
        return self.__open_statement

    @property
    def insert_statement(self):
        if not self.__insert_statement:
            self.__insert_statement = self.table.insert().compile()
        return self.__insert_statement

    @property
    def update_statement(self):
        if not self.__update_statement:
            conditions = []
            for key in self.identity_properties:
                 # Prefix with "b_" like adviced from sqlalchemy because
                 # of reserved names
                 conditions.append(self.table.columns[key] == sqlexpr.bindparam("b_%s" % key))
            statement = self.table.update().where(sqlexpr.and_(*conditions))
            self.__update_statement = statement.compile()
        return self.__update_statement


    @property
    def _generated_keys(self):
        for key, value in self.definitions.iteritems():
            if value.db_gen:
                yield key

    def save(self, *args):
        connection = self.table.bind.connect()
        transaction = connection.begin()
        try:
            # Try to open the item
            inserts = []
            updates = []
            for item in args:
                item_dict = dict((key, None if value is DEFAULT_VALUE else value) for key, value in item.iteritems())
                id = dict(("b_%s" % key, item_dict[key])
                       for key in self.identity_properties if key in item_dict)
                results = self.open_statement.execute(id)
                olditem = next(iter(results), None)
                if olditem is None:
                    inserts.append(dict(item_dict))
                else:
                    values = dict(item_dict)
                    values.update(id)
                    updates.append(values)
            if inserts:
                values = connection.execute(self.insert_statement, inserts)
                if len(args) == 1:
                    for key, value in zip(self._generated_keys,
                            values.inserted_primary_key):
                       item[key] = value
            if updates:
                values = connection.execute(self.update_statement, updates)
            transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            connection.close()

    def delete(self, item):
        connection = self.table.bind.connect()
        transaction = connection.begin()
        try:
            result = connection.execute(self.table.delete().where(
                    self._to_pk_where_clause(item)))
            if result.rowcount > 1:
                transaction.rollback()
                raise ValueError("There is more than one item to delete!")
            else:
                transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            connection.close()

    def _is_same_db(self, other_type):
        return other_type.corn is None or (isinstance(other_type.corn, Alchemy)\
                and other_type.corn.url == self.url)

    def is_all_alchemy(self, request, contexts=()):
        used_types = request.used_types()
        all_requests = reduce(lambda x, y: list(x) + list(y), used_types.values(), set())
        return all(isinstance(x, AlchemyWrapper) for x in all_requests) and\
                all(self._is_same_db(x) for x in used_types.keys())

    def execute(self, request, contexts=()):
        wrapped_request = self.dialect.wrap_request(request)
        # TODO: try to split the request if something is not managed

        if self.is_all_alchemy(wrapped_request, contexts):
            try:
                wrapped_request.is_valid(contexts)
            except InvalidRequestException as e:
                invalid_request = e.request.wrapped_request
                def predicate(req):
                    return req is invalid_request
                managed, not_managed = cut_on_predicate(request, predicate,
                        recursive=True)
                if managed:
                    result = self.execute(managed)
                else:
                    result = self._all()
                return python_executor.execute(not_managed, (result,))
            tables = wrapped_request.extract_tables()
            sql_query = sqlexpr.select(from_obj=tables)
            sql_query = wrapped_request.to_alchemy(sql_query, contexts)
            return_type = wrapped_request.return_type()
            print_sql(unicode(sql_query))
            try:
                connection = self.table.bind.connect()
                sql_result = connection.execute(sql_query)
                if return_type.type != list:
                    sql_result = next(iter(sql_result), None)
                    if isinstance(wrapped_request, OneWrapper) and sql_result is None:
                        if wrapped_request.default:
                            value = python_executor.execute(wrapped_request.default.wrapped_request)
                            return value
                        raise ValueError('.one() on an empty sequence')
                else:
                    sql_result = sql_result.fetchall()
            finally:
                connection.close()
            return self.dialect._transform_result(sql_result, return_type, self)
        else:
            return python_executor.execute(request)


from .dialects import get_dialect
from .wrappers import OneWrapper, AlchemyWrapper
