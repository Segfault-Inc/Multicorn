from ...requests import requests, wrappers
from . import Alchemy

from sqlalchemy import sql as sqlexpr


class AlchemyWrapper(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()

    def to_alchemy(self, query, contexts=()):
        raise NotImplementedError()




@AlchemyWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        contexts = contexts + (self.subject.return_type(contexts).inner_type,)
        query = self.subject.to_alchemy(query, contexts=())
        where_clause = self.predicate.to_alchemy(query, contexts)
        return query.where(where_clause)


@AlchemyWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.storage.table.select()


@AlchemyWrapper.register_wrapper(requests.EqRequest)
class EqWrapper(wrappers.BooleanOperationWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.subject.to_alchemy(query, contexts) ==\
                self.other.to_alchemy(query, contexts)

@AlchemyWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(wrappers.ContextWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return contexts[self.scope_depth - 1].to_alchemy(query,
                contexts[:self.scope_depth-1])

@AlchemyWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return self.value


@AlchemyWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        type = self.return_type(contexts)
        if type.corn and isinstance(type.corn, Alchemy):
            return type.corn.table.c[self.attr_name]
        else:
            raise ValueError("Cannot get attr on this!")


@AlchemyWrapper.register_wrapper(requests.AndRequest)
class AndWrapper(wrappers.AndWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return sqlexpr.and_(self.subject.to_alchemy(query, contexts),
                    self.other.to_alchemy(query, contexts))

@AlchemyWrapper.register_wrapper(requests.OrRequest)
class OrWrapper(wrappers.OrWrapper, AlchemyWrapper):

    def to_alchemy(self, query, contexts=()):
        return sqlexpr.or_(self.subject.to_alchemy(query, contexts),
                    self.other.to_alchemy(query, contexts))

