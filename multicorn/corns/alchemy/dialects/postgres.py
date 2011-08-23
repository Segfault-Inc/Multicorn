from .. import wrappers
from .. import InvalidRequestException
from ....requests import requests, types, CONTEXT as c, helpers

from sqlalchemy import sql as sqlexpr
from sqlalchemy.types import Unicode
from sqlalchemy.sql import expression
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import ARRAY


class substr(expression.FunctionElement, expression.ColumnElement):
    type = Unicode()
    name = 'substr'

@compiles(substr)
def default_substr(element, compiler, **kw):
    return compiler.visit_function(element)

class array(expression.ColumnElement):
    type = ARRAY(None)

@compiles(array)
def default_array(element, compiler, **kw):
    arg1 = element.clauses[0]
    return "ARRAY[%s]" % compiler.process(arg1)


def convert_tuple(datum, cursor):
    datum = datum.strip('(')
    current_token = ""
    elems = []
    in_string = False
    escape = False
    for a in datum:
        if in_string:
            if not escape:
                if a == '"':
                    in_string = False
                    elems.append(current_token)
                    current_token = ""
                else:
                    current_token += a
            elif a == '\\':
                escape = True
        elif a in (',', ')'):
            elems.append(current_token)
            current_token = ""
        else:
            current_token += a
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
    converter = psycopg2.extensions.string_types[1015]
    TUPLE_ARRAY = psycopg2.extensions.new_type((2287,), "TUPLEARRAY", convert_tuple_array)
    TUPLE = psycopg2.extensions.new_type((2249,), "TUPLE", convert_tuple)
    psycopg2.extensions.register_type(TUPLE_ARRAY)
    psycopg2.extensions.register_type(TUPLE)

except:
    print "Warning: postgresql driver not found"


#Base (dummy) array converter


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
                    req = req.with_only_columns([sqlexpr.tuple_(*[c.proxies[-1] for c in req.c]).label('__array__elem__')])
                    req = req.correlate(*query._froms)
                    select_expr = sqlexpr.expression.func.ARRAY(req.as_scalar())
                # If it is a dict, ensure that names dont collide
                elif return_type.type == dict:
                    tuple_elems = []
                    for c in req.c:
                        tuple_elems.append(c.proxies[-1])
                    select_expr = sqlexpr.tuple_(*tuple_elems)
                    query = req
                else:
                    select_expr = list(req.c)[0].proxies[-1]
                    query = req
                selects.append(select_expr.label(key))
            else:
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

@PostgresWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, PostgresWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        return_type = self.subject.return_type(wrappers.type_context(contexts))
        idx = sorted(return_type.mapping.keys()).index(self.attr_name)
        return self._extract_attr(query, idx)

    def _extract_attr(self, query, idx):
        values = None
        if hasattr(query, 'c'):
            values = sorted(list(query.c), key=lambda x : x.name)
        elif hasattr(query, 'clauses'):
            values = query.clauses
        if values is None:
            return self._extract_attr(query.element, idx)
        return values[idx].proxies[-1]

def select_to_tuple(query):
    if len(list(getattr(query, 'c', []))) > 1:
        return sqlexpr.tuple_(*[col.proxies[-1] for col in list(query.c)])
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
        subject_type = self.subject.return_type(wrappers.type_context(contexts))
        other_type = self.other.return_type(wrappers.type_context(contexts))
        need_subquery = False
        if isinstance(other_type, types.Dict):
            for value in other_type.mapping.values():
                if value.type == list:
                    need_subquery = True
                    break
        other_base = query.with_only_columns([])
        if need_subquery:
            subject = self.subject.to_alchemy(query.alias().select(), contexts)
            alias = query.alias().select()
            columns = [sqlexpr.literal_column(unicode(c.proxies[-1].label(c.name)))
                    for c in alias.c]
            other_base = alias.with_only_columns(columns)
            contexts = contexts[:-1] + (wrappers.Context(other_base,
                contexts[-1].type),)
            # If we have things to do on the list of elements,
            # append a filter after the context request
            other = self.other.to_alchemy(other_base, contexts)
            other = sqlexpr.select(from_obj=[other.alias(), subject.alias()])
        else:
            other = self.other.to_alchemy(other_base, contexts)
            subject = self.subject.to_alchemy(query, contexts)
        # Dict addition is a mapping merge
        if all(isinstance(x, types.Dict) for x in (subject_type, other_type)):
            columns = []
            for member in (subject, other):
                for c in sorted(member.c, key=lambda x : x.name):
                    columns.append(c.proxies[-1])
            columns = sorted(columns, key=lambda c: c.name)
            other = other.with_only_columns(columns)
            return other
        elif all(isinstance(x, types.List) for x in (subject_type, other_type)):
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
    pass

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
            return_type = aggregate.return_type(wrappers.type_context(contexts) + (type,))
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
        if not (isinstance(type, types.List) or issubclass(type.type, basestring)):
            raise InvalidRequestException(self,
                    "Slice is not managed on not list or string objects")
