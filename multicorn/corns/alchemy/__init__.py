# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from ..abstract import AbstractCorn
from ...requests.types import Type, List, Dict
from ...requests.helpers import cut_on_predicate
from ... import python_executor

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


class Alchemy(AbstractCorn):

    def __init__(self, name, identity_properties=None, url="sqlite:///",
            tablename=None, schema=None, engine_opts=None, create_table=True):
        super(Alchemy, self).__init__(name, identity_properties)
        self.__table = None
        self.url = url
        self.tablename = tablename or name
        self.schema = schema
        self.engine_opts = engine_opts or {}
        self.create_table = True

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
        self.dialect = get_dialect(engine)

    def register(self, name, type):
        self.properties[name] = Type(corn=self, name=name, type=type)

    @property
    def table(self):
        if self.__table is not None:
            return self.__table
        columns = []
        for prop in self.properties.values():
            kwargs = {}
            if prop.name in self.identity_properties:
                kwargs["primary_key"] = True
            type = self.dialect.alchemy_type(prop)
            column = Column(prop.name, type, **kwargs)
            columns.append(column)
        kwargs = dict(useexisting=True)
        if self.schema:
            kwargs['schema'] = self.schema
        table = Table(self.tablename, self.metadata, *columns,
                **kwargs)
        if self.create_table == True:
            table.create()
        self.__table = table
        return table

    def _all(self):
        return self.table.select().execute()

    def _to_pk_where_clause(self, item):
        conditions = []
        for key in self.identity_properties:
             conditions.append(self.table.columns[key] == item[key])
        return sqlexpr.and_(*conditions)

    def save(self, item):
        connection = self.table.bind.connect()
        transaction = connection.begin()
        # Try to open the item
        where_clause = self._to_pk_where_clause(item)
        statement = self.table.select().where(where_clause)
        results = statement.execute()
        try:
            iter(results).next()
            # The item exists, it's an UPDATTE
            rows = self.table.update().where(where_clause).values(dict(item)).execute()
            if rows.rowcount > 1:
                transaction.rollback()
                raise ValueError("There is more than one item to update!")
            transaction.commit()
        except StopIteration:
            # The item does not exist, it's an INSERT
            statement = self.table.insert().values(dict(item)).execute()
            transaction.commit()

    def delete(self, item):
        connection = self.table.bind.connect()
        transaction = connection.begin()
        result = self.table.delete().where(
                self._to_pk_where_clause(item)).execute()
        if result.rowcount > 1:
            transaction.rollback()
            raise ValueError("There is more than one item to delete!")

    def _is_same_db(self, other_type):
        return other_type.corn is None or (isinstance(other_type.corn, Alchemy)\
                and other_type.corn.url == self.url)

    def is_all_alchemy(self, request, contexts=()):
        used_types = request.used_types()
        all_requests = reduce(lambda x, y: list(x) + list(y), used_types.values(), set())
        return all(isinstance(x, self.dialect.RequestWrapper) for x in all_requests) and\
                all(self._is_same_db(x) for x in used_types.keys())

    def _transform_result(self, result, return_type):
        def process_list(result):
            for item in result:
                yield self._transform_result(item, return_type.inner_type)
        if isinstance(return_type, List):
            return process_list(result)
        elif return_type.type == dict:
            if return_type == self.type:
                result = dict(((key, value) for key, value in dict(result).iteritems()
                    if key in self.properties))
                return self.create(result)
            elif return_type.corn:
                return return_type.corn.create(result)
            else:
                newdict = {}
                for key, type in return_type.mapping.iteritems():
                    # Even for dicts, sql returns results "inline"
                    if isinstance(type, Dict):
                        subresult = {}
                        for subkey in result.keys():
                            subresult[subkey.replace('__%s_' % key,'').strip('__')] = result[subkey]
                        newdict[key] = self._transform_result(subresult, type)
                    elif isinstance(type, List):
                        newdict[key] = self._transform_result(result, type)
                    else:
                        newdict[key] = result[key]
                return newdict
        else:
            result = list(result)
            if len(result) > 1:
                raise ValueError('More than one element in .one()')
            if len(result) == 0:
                raise ValueError('.one() on an empty sequence')
            return result[0]

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
            sql_result = sql_query.execute()
            if return_type.type != list:
                sql_result = next(iter(sql_result), None)
                if isinstance(wrapped_request, OneWrapper) and sql_result is None:
                    if request.default_value:
                        return python_executor.execute(request.default_value,
                                (List(return_type)))
                    raise ValueError('.one() on an empty sequence')
            print(unicode(sql_query))
            return self._transform_result(sql_result, return_type)
        else:
            return python_executor.execute(request)


from .dialects import get_dialect
from .wrappers import OneWrapper
