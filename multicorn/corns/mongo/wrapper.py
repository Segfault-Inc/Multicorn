from ...requests import requests, wrappers
from .request import MongoRequest


class MongoWrapper(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()

    def to_mongo(self, contexts=()):
        raise NotImplementedError()


@MongoWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        return MongoRequest()


@MongoWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        expression = self.subject.to_mongo()
        expression.spec.update(self.predicate.to_mongo())
        return expression


@MongoWrapper.register_wrapper(requests.EqRequest)
class EqWrapper(wrappers.BooleanOperationWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        return {self.subject.to_mongo(contexts): self.other.to_mongo(contexts)}


@MongoWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        return self.value


@MongoWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, MongoWrapper):

    def to_mongo(self, query, contexts=()):
        return self.attr_name


@MongoWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(wrappers.ContextWrapper, MongoWrapper):

    def to_mongo(self, query, contexts=()):
        return contexts[self.scope_depth - 1].query


@MongoWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(wrappers.LenWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        if not contexts:
            expression = self.subject.to_mongo()
            expression.count = True
            return expression
        else:
            raise NotImplementedError("Non final len()")


@MongoWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(wrappers.OneWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        if not contexts:
            expression = self.subject.to_mongo()
            expression.one = True
            return expression
        else:
            raise NotImplementedError("Non final one()")
