from .. import wrappers
from .. import InvalidRequestException
from ....requests import requests

from sqlalchemy import sql as sqlexpr

class PostgresWrapper(wrappers.AlchemyWrapper):
    class_map = wrappers.AlchemyWrapper.class_map.copy()


@PostgresWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(PostgresWrapper, wrappers.DictWrapper):

    def to_alchemy(self, query, contexts=()):
        selects = []
        for key, request in sorted(self.value.iteritems(), key=lambda x: x[0]):
            return_type = request.return_type(wrappers.type_context(contexts))
            req = request.to_alchemy(query, contexts)
            if return_type.type == list:
                raise ValueError("SQLAlchemy cannot return\
                        lists as part of a dict")
            elif isinstance(req, sqlexpr.Selectable):
                # If it is a dict, ensure that names dont collide
                if return_type.type == dict:
                    tuple_elems = []
                    for c in req.c:
                        tuple_elems.append(c.proxies[-1])
                    selects.append(sqlexpr.tuple_(*tuple_elems).label(key))
                else:
                    selects.append(list(req.c)[0].proxies[-1].label(key))
                query = req.correlate(query)
            else:
                selects.append(req.label(key))
        return query.with_only_columns(selects)


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
    pass

@PostgresWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, PostgresWrapper):
    pass

@PostgresWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, PostgresWrapper):
    pass

class BinaryOperationWrapper(PostgresWrapper):
    pass

@PostgresWrapper.register_wrapper(requests.AndRequest)
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
    pass

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
    pass

@PostgresWrapper.register_wrapper(requests.SortRequest)
class SortWrapper(wrappers.SortWrapper, PostgresWrapper):
    pass

@PostgresWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(wrappers.OneWrapper, PostgresWrapper):
    pass

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
    pass
