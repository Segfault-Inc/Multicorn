# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from .. import wrappers
from .. import InvalidRequestException
from ..dialects import BaseDialect
from ....requests import requests, types, CONTEXT as c
from multicorn.utils import colorize
from datetime import datetime, date

from sqlalchemy import sql as sqlexpr
from sqlalchemy.types import Unicode, UserDefinedType
from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import ARRAY

from sqlalchemy.engine.base import RowProxy


class substr(expression.FunctionElement, expression.ColumnElement):
    type = Unicode()
    name = 'substr'


@compiles(substr)
def default_substr(element, compiler, **kw):
    return compiler.visit_function(element)


def convert_tuple(datum, cursor):
    datum = datum.decode(cursor.connection.encoding)
    datum = datum.lstrip('(')
    if datum.startswith('"{"'):
        return convert_tuple_array(datum.strip('"'), cursor)

    isnull = True
    current_token = u""
    elems = []
    in_string = False
    escape = False
    datum = iter(datum)
    stop_marker = object()
    a = next(datum)
    while a != stop_marker:
        if in_string:
            if not escape:
                if a == '"':
                    in_string = False
                    elems.append(current_token)
                    isnull = True
                    a = next(datum, stop_marker)
                    current_token = u""
                else:
                    current_token += a
            elif a == '\\':
                escape = True

        elif a == '"':
            in_string = True
            isnull = False
        elif a in (',', ')'):
            elems.append(None if isnull else current_token)
            isnull = True
            current_token = u""
        else:
            isnull = False
            current_token += a
        a = next(datum, stop_marker)
    return tuple(elems)


def convert_tuple_array(data, cursor):
    """Convert an array of tuple from a pg string
    to a python array of tuple"""
    data = converter(data, cursor)
    tuples = []
    for datum in data:
        tuples.append(convert_tuple(datum, cursor))
    return tuples

try:
    import psycopg2
    type_map = {
        unicode: psycopg2.extensions.string_types[1043],
        datetime: psycopg2.extensions.string_types[1184],
        date: psycopg2.extensions.string_types[1082],
        int: psycopg2.extensions.string_types[23]
    }
    converter = psycopg2.extensions.string_types[1015]
    unicode_converter = psycopg2.extensions.string_types[1043]
    TUPLE_ARRAY = psycopg2.extensions.new_type(
        (2287,), "TUPLEARRAY", convert_tuple_array)
    TUPLE = psycopg2.extensions.new_type((2249,), "TUPLE", convert_tuple)
    psycopg2.extensions.register_type(TUPLE_ARRAY)
    psycopg2.extensions.register_type(TUPLE)
except:
    import sys
    print(colorize(
        'yellow',
        "WARNING: Postgresql driver not found"), file=sys.stderr)


class Tuple(UserDefinedType):

    def __init__(self):
        pass

    def get_col_spec(self):
        pass

    def result_processor(self, dialect, coltype):
        def processor(value):
            return value
        return processor


class array(expression.ColumnElement):

    type = ARRAY(Tuple())

    def __init__(self, element):
        self.element = element
        self.clauses = [c.proxies[-1] if isinstance(c, expression._Label)
                else c  for c in self.element.clauses]


@compiles(array)
def default_array(element, compiler, **kw):
    arg1 = element.element
    if isinstance(arg1.type, ARRAY):
        return "ARRAY(select row(%s))" % compiler.process(arg1)
    return "ARRAY(%s)" % compiler.process(arg1)


class array_elem(expression.ColumnElement):

    type = Tuple()

    def __init__(self, element):
        if len(element.c) == 1:
            col_base = element._raw_columns[0]
            if hasattr(col_base, 'element'):
                col_base = col_base.element
        else:
            col_base = element
        col_base = element
        self.clauses = [c.proxies[-1] for c in col_base.c]
        self.element = element


@compiles(array_elem)
def default_array_elem(element, compiler, **kw):
    return compiler.process(element.element.with_only_columns(
        [tuple_(*element.clauses)]))


class tuple_(expression.ColumnElement):

    type = Tuple()

    def __init__(self, *clauses):
        self.clauses = clauses

    @property
    def c(self):
        return sqlexpr.ColumnCollection(*self.clauses)


@compiles(tuple_)
def default_tuple_(element, compiler, **kw):
    return "(%s)" % compiler.process(sqlexpr.tuple_(*element.clauses))


class PostgresWrapper(wrappers.AlchemyWrapper):
    class_map = wrappers.AlchemyWrapper.class_map.copy()


@PostgresWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(PostgresWrapper, wrappers.DictWrapper):

    def to_alchemy(self, query, contexts=()):
        selects = []
        for key, request in sorted(self.value.iteritems(), key=lambda x: x[0]):
            return_type = request.return_type(wrappers.type_context(contexts))
            req = request.to_alchemy(query, contexts)
            if isinstance(req, sqlexpr.Select):
                if return_type.type == list:
                    select_expr = array(array_elem(req))
                # If it is a dict, ensure that names dont collide
                elif return_type.type == dict:
                    tuple_elems = []
                    for c in req.c:
                        tuple_elems.append(c.proxies[-1])
                    select_expr = tuple_(*tuple_elems)
                    query = req
                else:
                    select_expr = list(req.c)[0].proxies[-1]
                    query = req
                selects.append(select_expr.label(key))
            else:
#                if hasattr(req, 'table'):
#                    query.append_from(req.table)
                selects.append(req.label(key))
        return query.with_only_columns(selects)

    def is_valid(self, contexts):
        for req in self.value.values():
            req.is_valid(contexts)


@PostgresWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(wrappers.ContextWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.StrRequest)
class StrWrapper(wrappers.StrWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.UpperRequest)
class UpperWrapper(wrappers.UpperWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.LowerRequest)
class LowerWrapper(wrappers.LowerWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.RegexRequest)
class RegexRequest(wrappers.RegexWrapper, PostgresWrapper):

    def to_alchemy(self, query, contexts=()):
        if self.value:
            # Validation succeeded, we have an "equivalent" like
            # construct
            return super(RegexRequest, self).to_alchemy(query, contexts)
        else:
            subject = self.subject.to_alchemy(query, contexts)
            other = self.other.to_alchemy(query, contexts)
            return subject.op('~')(other)

    def is_valid(self, contexts=()):
        try:
            super(RegexRequest, self).is_valid(contexts)
        except InvalidRequestException as e:
            if e.request != self:
                raise
            else:
                # Failure occured from unability to transform to like
                # construct, silently ignore and fall back to a regexp
                self.value = None
                pass


@PostgresWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.ListRequest)
class ListWrapper(wrappers.ListWrapper, LiteralWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, PostgresWrapper):

    def to_alchemy(self, query, contexts=()):
        newquery = self.subject.to_alchemy(query, contexts)
        return_type = self.subject.return_type(wrappers.type_context(contexts))
        if return_type.corn is not None:
            # We work directly on a type, better do something good with
            # it!
            keys = return_type.corn.definitions.keys()
        else:
            keys = return_type.mapping.keys()
        idx = sorted(keys).index(self.attr_name)
        return self._extract_attr(newquery, idx)

    def _extract_attr(self, query, idx):
        values = None
        if hasattr(query, 'c'):
            values = sorted(list(query.c), key=lambda x: x.name)
        elif isinstance(query, array):
            return query
        elif hasattr(query, 'clauses'):
            values = [c for c in query.clauses]
        elif isinstance(query, expression._Label):
            return self._extract_attr(query.element, idx)
        elif hasattr(query, 'proxies'):
            newquery = query.proxies[-1]
            if isinstance(newquery.type, (Tuple, ARRAY)):
                truc = self._extract_attr(newquery, idx)
                return truc
            return newquery
        elif isinstance(query, sqlexpr.ColumnElement):
            return query
        if values is None:
            return self._extract_attr(query.element, idx)
        elif len(values) == 1:
            return self._extract_attr(values[0], idx)
        return values[idx].proxies[-1]


def select_to_tuple(query):
    if len(list(getattr(query, 'c', []))) > 1:
        return tuple_(*[col.proxies[-1] for col in list(query.c)])
    elif isinstance(query, sqlexpr.Selectable):
        return list(query.c)[0].proxies[-1]
    else:
        return query


class BinaryOperationWrapper(PostgresWrapper, wrappers.BinaryOperationWrapper):

    def left_column(self, query, contexts):
        subject = self.subject.to_alchemy(query, contexts)
        return select_to_tuple(subject)

    def right_column(self, query, contexts):
        other = self.other.to_alchemy(query, contexts)
        return select_to_tuple(other)


@PostgresWrapper.register_wrapper(requests.InRequest)
class InWrapper(wrappers.InWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.AndRequest)
class AndWrapper(wrappers.AndWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.OrRequest)
class OrWrapper(wrappers.OrWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.EqRequest)
class EqWrapper(wrappers.EqWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.NeRequest)
class NeWrapper(wrappers.NeWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.LtRequest)
class LtWrapper(wrappers.LtWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.GtRequest)
class GtWrapper(wrappers.GtWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.LeRequest)
class LeWrapper(wrappers.LeWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.GeRequest)
class GeWrapper(wrappers.GeWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.AddRequest)
class AddWrapper(wrappers.AddWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        subject_type = self.subject.return_type(
            wrappers.type_context(contexts))
        other_type = self.other.return_type(wrappers.type_context(contexts))
        need_subquery = False
        if isinstance(other_type, types.Dict):
            need_subquery = True
        if need_subquery:
            subject = self.subject.to_alchemy(query.alias().select(), contexts)
            contexts = contexts[:-1] + (wrappers.Context(subject,
                contexts[-1].type),)
            other = self.other.to_alchemy(query, contexts)
            other = other.correlate(*subject._froms)
            columns = []
            for c in sorted(list(other.c), key=lambda x: x.name):
                column = None
                for proxy in c.proxies:
                    column = proxy
                    if isinstance(proxy, array):
                        column = array(array_elem(
                            proxy.element.element.correlate(*subject._froms)))
                        break
                columns.append(other.with_only_columns(
                    [column]).correlate(*subject._froms)
                    .as_scalar().label(c.name))
            columns = sorted(columns + subject._raw_columns,
                    key=lambda x: x.name)
            return subject.with_only_columns(columns)
        else:
            other_base = query.with_only_columns([])
            other = self.other.to_alchemy(other_base, contexts)
            subject = self.subject.to_alchemy(query, contexts)
            base_request = other
        # Dict addition is a mapping merge
            if all(isinstance(x, types.Dict)
                   for x in (subject_type, other_type)):
                columns = []
                for member in (subject, other):
                    for c in sorted(member.c, key=lambda x: x.name):
                        columns.append(c.proxies[-1])
                columns = sorted(columns, key=lambda c: c.name)
                other = base_request.with_only_columns(columns)
                return other
            elif all(isinstance(x, types.List)
                     for x in (subject_type, other_type)):
                return subject.union(other)
            else:
                return subject + other


@PostgresWrapper.register_wrapper(requests.SubRequest)
class SubWrapper(wrappers.SubWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.MulRequest)
class MulWrapper(wrappers.MulWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.DivRequest)
class DivWrapper(wrappers.DivWrapper, BinaryOperationWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.MapRequest)
class MapWrapper(wrappers.MapWrapper, PostgresWrapper):

    def to_alchemy(self, query, contexts=()):
        newquery = self.subject.to_alchemy(query, contexts)
        type = self.subject.return_type(wrappers.type_context(contexts))
        contexts = contexts + (wrappers.Context(newquery, type.inner_type),)
        select = self.new_value.to_alchemy(newquery, contexts)
        # When we have a subselect as a scalar, un-nest it to take
        # the base query
        if isinstance(select, expression._Label):
            select = select.element
        if isinstance(select, array):
            base_query = select.element.element
            # An array is based on a tuple, or at least a single column
            column = list(select.element.element.c)[0].proxies[-1]
            if isinstance(column, tuple_):
                clauses = column.clauses
            else:
                clauses = [column.proxies[-1]]
            return base_query.with_only_columns(clauses).alias()\
                .select().correlate(*base_query._froms)
        while not isinstance(newquery, sqlexpr.Select):
            newquery = newquery.element
        if not isinstance(select, expression.Selectable):
            if not hasattr(select, '__iter__'):
                select = [select]
            return newquery.with_only_columns(select)
        return select


@PostgresWrapper.register_wrapper(requests.GroupbyRequest)
class GroupbyWrapper(wrappers.GroupbyWrapper, PostgresWrapper):

    def to_alchemy(self, query, contexts=()):

        query = self.subject.to_alchemy(query, contexts).alias().select()
        type = self.subject.return_type(wrappers.type_context(contexts))
        group_key = self.key.to_alchemy(query, contexts +
                (wrappers.Context(query, type.inner_type),))
        key = select_to_tuple(group_key).label('key')
        if isinstance(group_key, expression.Select):
            group_key = [col.proxies[-1] for col in group_key.c]
        else:
            group_key = [group_key]
        replacements = {}
        self.aggregates = self.from_request(self.aggregates._copy_replace({}))
        need_subquery = False
        for aggregate in self.aggregates.value.values():
            return_type = aggregate.return_type(
                wrappers.type_context(contexts) + (type,))
            if return_type.type == list:
                # If we plan on returning a list, we need a subquery
                need_subquery = True

                # If we have things to do on the list of elements,
                # append a filter after the context request
                chain = requests.as_chain(aggregate.wrapped_request)
                if isinstance(chain[0], requests.ContextRequest):
                    replacements[chain[0]] = chain[0].filter(
                        c(-2).key == c.key)
        if replacements:
            aggregates = self.aggregates.wrapped_request._copy_replace(
                    replacements)
            aggregates = self.from_request(aggregates)
        else:
            aggregates = self.aggregates
        newquery = query.column(key)
        if need_subquery:
            newquery = newquery.alias().select()
        key_type = self.key.return_type(wrappers.type_context(contexts) +
                    (type.inner_type,))
        key_type = types.Dict({'key': key_type})
        if isinstance(type.inner_type, types.Dict):
            new_dict = dict(type.inner_type.mapping)
        else:
            new_dict = {}
        new_dict['key'] = key_type
        new_type = types.List(types.Dict(new_dict))
        args = (query, contexts +
                (wrappers.Context(query, type),
                wrappers.Context(query.with_only_columns([key]), key_type),
                wrappers.Context(newquery, new_type)))
        group = aggregates.to_alchemy(*args)
        group = group.group_by(*group_key)
        columns = sorted(list(group._raw_columns) + [key.label('key')],
                key=lambda x: x.name)
        group = group.with_only_columns(columns)
        return group


@PostgresWrapper.register_wrapper(requests.SortRequest)
class SortWrapper(wrappers.SortWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(wrappers.OneWrapper, PostgresWrapper):

    def is_valid(self, contexts):
        self.subject.is_valid(contexts)

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        return query.limit(1)


class AggregateWrapper(wrappers.AggregateWrapper, PostgresWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(wrappers.LenWrapper, AggregateWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.SumRequest)
class SumWrapper(wrappers.SumWrapper, AggregateWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.MaxRequest)
class MaxWrapper(wrappers.MaxWrapper, AggregateWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.MinRequest)
class MinWrapper(wrappers.MinWrapper, AggregateWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.DistinctRequest)
class DistinctWrapper(wrappers.DistinctWrapper, AggregateWrapper):
    pass


@PostgresWrapper.register_wrapper(requests.SliceRequest)
class SliceWrapper(wrappers.SliceWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        type = self.subject.return_type(contexts)
        query = self.subject.to_alchemy(query, contexts)
        if isinstance(type, types.List):
            if self.slice.stop:
                stop = self.slice.stop - (self.slice.start or 0)
                query = query.limit(stop)
            if self.slice.start:
                query = query.offset(self.slice.start)
            return query.alias().select()
        elif type.type in (unicode, str):
            start = self.slice.start or 0
            if isinstance(query, expression.Select):
                query = list(query.c)[0].proxies[-1]
            args = [start + 1]
            if self.slice.stop is not None:
                stop = self.slice.stop - start
                args.append(stop)
            return substr(query, *args)

    def is_valid(self, contexts=()):
        self.basic_check(contexts)
        type = self.subject.return_type(contexts)
        if not (isinstance(type, types.List) or
                issubclass(type.type, basestring)):
            raise InvalidRequestException(self,
                    "Slice is not managed on not list or string objects")


class PostgresDialect(BaseDialect):

    RequestWrapper = PostgresWrapper

    def _transform_result(self, result, return_type, corn):
        def process_list(result):
            if isinstance(result, RowProxy) and isinstance(result._row, tuple):
                result = result._row[0]
            for item in result:
                yield self._transform_result(
                    item, return_type.inner_type, corn)
        if isinstance(return_type, types.List):
            return process_list(result)
        elif return_type.type == dict:
            newdict = {}
            if return_type.corn:
                ordered_dict = sorted(((x, y.type)
                        for x, y in return_type.corn.definitions.iteritems()),
                        key=lambda x: x[0])
            else:
                ordered_dict = sorted(
                    return_type.mapping.iteritems(), key=lambda x: x[0])
            if result is None:
                return None
            # Temporary fix for one value tuples
            if not isinstance(result, (RowProxy, tuple)):
                result = tuple([result])
            for idx, (key, type) in enumerate(ordered_dict):
                # Even for dicts, sql returns results "inline"
                newdict[key] = self._transform_result(result[idx], type, corn)
            if return_type.corn:
                return return_type.corn.create(newdict)
            else:
                return newdict
        else:
            if hasattr(result, '__iter__'):
                result = list(result)
                if len(result) > 1:
                    raise ValueError('More than one element in .one()')
                if len(result) == 0:
                    raise ValueError('.one() on an empty sequence')
                return result[0]
            else:
                if (result is not None and not
                    isinstance(result, return_type.type) and
                    isinstance(result, basestring)):
                    postgres_type = type_map.get(return_type.type, None)
                    if postgres_type:
                        return postgres_type(result, None)
                    else:
                        return return_type.type(result)
                else:
                    return result

    def wrap_request(self, request):
        return self.RequestWrapper.from_request(request)
