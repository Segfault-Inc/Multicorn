from ...requests import requests, wrappers, types
from . import Alchemy, InvalidRequestException

from sqlalchemy import sql as sqlexpr
from sqlalchemy.sql import expression


class Context(object):

    def __init__(self, query, type):
        self.type = type
        self.query = query


def type_context(context):
    return tuple([c.type for c in context])

class AlchemyWrapper(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()

    def to_alchemy(self, query, contexts=()):
        raise NotImplementedError()

    def extract_tables(self):
        raise NotImplementedError()

    def is_valid(self, contexts=()):
        raise NotImplementedError()


@AlchemyWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        type = self.subject.return_type(type_context(contexts))
        contexts = contexts + (Context(query, type.inner_type),)
        where_clause = self.predicate.to_alchemy(query, contexts)
        return query.where(where_clause)

    def extract_tables(self):
        return self.subject.extract_tables() + self.predicate.extract_tables()

    def is_valid(self, contexts=()):
        self.subject.is_valid(contexts)
        self.predicate.is_valid(contexts +
                (self.subject.return_type(contexts).inner_type,))


@AlchemyWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        for c in self.aliased_table.c:
            query = query.column(c.label(c.name))
        if contexts:
            return query.apply_labels()
        return query

    def extract_tables(self):
        self.aliased_table = self.storage.table.alias()
        return (self.aliased_table,)

    def is_valid(self, contexts=()):
        pass

@AlchemyWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(wrappers.ContextWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return contexts[self.scope_depth - 1].query

    def extract_tables(self):
        # A context switch does not introduce new tables
        return tuple()

    def is_valid(self, contexts=()):
        if len(contexts) <= self.scope_depth:
            raise InvalidRequestException(self, "Invalid Context Request:\
                    no %sth parent scope" % self.scope_depth)

@AlchemyWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.value

    def extract_tables(self):
        return tuple()

    def is_valid(self, contexts=()):
       # TODO: raise on invalid Types
       pass


@AlchemyWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        return query.c[self.attr_name].proxies[-1]

    def extract_tables(self):
        return self.subject.extract_tables()

    def is_valid(self, contexts=()):
        self.subject.is_valid(contexts)
        return_type = self.subject.return_type(contexts)
        # TODO: manage attr getters on date objects, maybe
        if not isinstance(return_type, types.Dict):
            raise InvalidRequestException(self, "Cannot access attribute in a \
                    non-dict context")


class BinaryOperationWrapper(AlchemyWrapper):

    def extract_tables(self):
        return self.subject.extract_tables() + self.other.extract_tables()

    def is_valid(self, contexts=()):
        self.subject.is_valid(contexts)
        self.other.is_valid(contexts)


@AlchemyWrapper.register_wrapper(requests.AndRequest)
class AndWrapper(wrappers.AndWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return sqlexpr.and_(self.subject.to_alchemy(query, contexts),
                    self.other.to_alchemy(query, contexts))


@AlchemyWrapper.register_wrapper(requests.OrRequest)
class OrWrapper(wrappers.OrWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return sqlexpr.or_(self.subject.to_alchemy(query, contexts),
                    self.other.to_alchemy(query, contexts))


@AlchemyWrapper.register_wrapper(requests.EqRequest)
class EqWrapper(wrappers.BooleanOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) ==\
                self.other.to_alchemy(query, contexts)


@AlchemyWrapper.register_wrapper(requests.NeRequest)
class NeWrapper(wrappers.BooleanOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) !=\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.LtRequest)
class LtWrapper(wrappers.BooleanOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) <\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.GtRequest)
class GtWrapper(wrappers.BooleanOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) >\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.LeRequest)
class LeWrapper(wrappers.BooleanOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) <=\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.GeRequest)
class GeWrapper(wrappers.BooleanOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) >=\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.AddRequest)
class AddWrapper(wrappers.AddWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        subject = self.subject.to_alchemy(query, contexts)
        other_base = query.with_only_columns([])
        other = self.other.to_alchemy(other_base, contexts)
        subject_type = self.subject.return_type(type_context(contexts))
        other_type = self.other.return_type(type_context(contexts))
        # Dict addition is a mapping merge
        if all(isinstance(x, types.Dict) for x in (subject_type, other_type)):
            for c in subject.c:
                other = other.column(c.proxies[-1])
            return other.correlate(subject)
        elif all(isinstance(x, types.List) for x in (subject_type, other_type)):
            return subject.union(other)
        else:
            return subject + other

@AlchemyWrapper.register_wrapper(requests.SubRequest)
class SubWrapper(wrappers.ArithmeticOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) -\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.MulRequest)
class MulWrapper(wrappers.ArithmeticOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) *\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.DivRequest)
class DivWrapper(wrappers.ArithmeticOperationWrapper, BinaryOperationWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) /\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.MapRequest)
class MapWrapper(wrappers.MapWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        type = self.subject.return_type(type_context(contexts))
        contexts = contexts + (Context(query, type.inner_type),)
        select = self.new_value.to_alchemy(query, contexts)
        if not isinstance(select, expression.Selectable):
            if not hasattr(select, '__iter__'):
                select = [select]
            return query.with_only_columns(select)
        return select

    def extract_tables(self):
        return self.subject.extract_tables() + self.new_value.extract_tables()

    def is_valid(self, contexts=()):
        self.subject.is_valid(contexts)
        return_type = self.subject.return_type(contexts)
        if not isinstance(return_type, types.List):
            raise InvalidRequestException(self, "Cannot apply map to the type\
                                                %s" % return_type.type)
        contexts = contexts + (return_type.inner_type,)
        self.new_value.is_valid(contexts)

@AlchemyWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(wrappers.DictWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        selects = []
        for key, request in self.value.iteritems():
            return_type = request.return_type(type_context(contexts))
            req = request.to_alchemy(query, contexts)
            if return_type.type == list:
                raise ValueError("SQLAlchemy cannot return\
                        lists as part of a dict")
            elif return_type.type == dict:
                # If it is a dict, ensure that names dont collide
                for c in req.c:
                    selects.append(c.proxies[-1].label("__%s_%s__" % (key, c.name)))
                query = req.correlate(query)
            else:
                selects.append(req.label(key))
        return query.with_only_columns(selects)

    def is_valid(self, contexts):
        for req in self.value.values():
            req.is_valid(contexts)
            return_type = req.return_type(contexts)
            if return_type.type == list:
                raise InvalidRequestException(self, "Cannot fetch a list\
                        as a mapping value")

    def extract_tables(self):
        return reduce(lambda x, y: x + y, (request.extract_tables()
                        for request in self.value.values()))


@AlchemyWrapper.register_wrapper(requests.GroupbyRequest)
class GroupbyWrapper(wrappers.GroupbyWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        type = self.subject.return_type(type_context(contexts))
        key = self.key.to_alchemy(query, contexts +
                (Context(query, type.inner_type),))
        group = self.aggregate.to_alchemy(query, contexts + (Context(query, type),))
        if isinstance(group, expression.Selectable):
            # We have multiple return columns
            if len(group.c) == 1:
                column = list(group.c)[0].proxies[-1].label('group')
                group = group.with_only_columns([column])
        else:
            group = query.column(group.label('group'))
        group = group.group_by(key)
        group = group.column(key.label('key'))
        return group

    def extract_tables(self):
        return self.subject.extract_tables() +\
               self.key.extract_tables() +\
               self.aggregate.extract_tables()

    def is_valid(self, contexts=()):
        self.subject.is_valid(contexts)
        type = self.subject.return_type(contexts)
        self.key.is_valid(contexts + (type.inner_type,))
        self.aggregate.is_valid(contexts + (type,))


@AlchemyWrapper.register_wrapper(requests.SortRequest)
class SortWrapper(wrappers.SortWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        type = self.subject.return_type(type_context(contexts))
        contexts = contexts + (Context(query, type.inner_type,),)
        keys = []
        for key, reverse in self.sort_keys:
            sqlkey = key.to_alchemy(query, contexts)
            if reverse:
                keys.append(sqlexpr.desc(sqlkey))
            else:
                keys.append(sqlexpr.asc(sqlkey))
        return query.order_by(*keys)

    def is_valid(self, contexts=()):
        self.subject.is_valid(contexts)
        contexts = contexts + (self.subject.return_type(contexts).inner_type,)
        for key, _ in self.sort_keys:
            key.is_valid(contexts)

    def extract_tables(self):
        return reduce(lambda x, y: x+y, [key.extract_tables()
            for key, _ in self.sort_keys], self.subject.extract_tables())

@AlchemyWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(wrappers.OneWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        if contexts:
            # We are in a subquery, no limit to append!
            return query
        return query.limit(1)

    def extract_tables(self):
        return self.subject.extract_tables()

    def is_valid(self, contexts):
        pass

class AggregateWrapper(AlchemyWrapper):


    def extract_tables(self):
        return self.subject.extract_tables()

    def is_valid(self, contexts):
        self.subject.is_valid(contexts)
        type = self.subject.return_type(contexts)
        if not isinstance(type, types.List):
            raise InvalidRequestException("Cannot perform a sum on something\
                    which isn't a list")


@AlchemyWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(wrappers.LenWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        return query.with_only_columns([expression.func.count(1)])


@AlchemyWrapper.register_wrapper(requests.SumRequest)
class SumWrapper(wrappers.AggregateWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        column = list(query.c)[0]
        return query.with_only_columns([expression.func.sum(column)])

@AlchemyWrapper.register_wrapper(requests.MaxRequest)
class MaxWrapper(wrappers.AggregateWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        column = list(query.c)[0]
        return query.with_only_columns([expression.func.max(column)])

@AlchemyWrapper.register_wrapper(requests.MinRequest)
class MinWrapper(wrappers.AggregateWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        column = list(query.c)[0]
        return query.with_only_columns([expression.func.min(column)])


@AlchemyWrapper.register_wrapper(requests.DistinctRequest)
class DistinctWrapper(wrappers.AggregateWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        query = self.subject.to_alchemy(query, contexts)
        return query.distinct()

@AlchemyWrapper.register_wrapper(requests.SliceRequest)
class SliceWrapper(wrappers.PreservingWrapper, AggregateWrapper):

    def to_alchemy(self, query, contexts=()):
        pass


