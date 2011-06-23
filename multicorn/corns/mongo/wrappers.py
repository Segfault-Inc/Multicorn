# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ...requests import requests, wrappers, types
from .request import MongoRequest
from .mapreduce import make_mr_map
from .ragequit import RageQuit
from .jsoner import to_json


class MongoWrapper(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()

    def to_mongo(self, contexts=()):
        raise NotImplementedError()


class BinaryMongoWrapper(MongoWrapper):
    pass


class ContextDefinerMongoWrapper(MongoWrapper):

    def sub(self, contexts):
        return contexts + (
            self.subject.return_type(contexts).inner_type,)


@MongoWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        return MongoRequest()


@MongoWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, ContextDefinerMongoWrapper):

    def to_mongo(self, contexts=()):
        mrq = self.subject.to_mongo(contexts)
        mrq.set_current_where(
            self.predicate.to_mongo(self.sub(contexts)))
        return mrq


@MongoWrapper.register_wrapper(requests.AndRequest)
class AndWrapper(wrappers.AndWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s && %s)" % (self.subject.to_mongo(contexts),
                               self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.OrRequest)
class OrWrapper(wrappers.OrWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s || %s)" % (self.subject.to_mongo(contexts),
                               self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.LeRequest)
class LeWrapper(wrappers.LeWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s <= %s)" % (self.subject.to_mongo(contexts),
                               self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.GeRequest)
class GeWrapper(wrappers.GeWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s >= %s)" % (self.subject.to_mongo(contexts),
                               self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.LtRequest)
class LtWrapper(wrappers.LtWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s < %s)" % (self.subject.to_mongo(contexts),
                              self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.GtRequest)
class GtWrapper(wrappers.GtWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s > %s)" % (self.subject.to_mongo(contexts),
                              self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.NeRequest)
class NeWrapper(wrappers.NeWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s != %s)" % (self.subject.to_mongo(contexts),
                               self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.EqRequest)
class EqWrapper(wrappers.BooleanOperationWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s == %s)" % (self.subject.to_mongo(contexts),
                               self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        return to_json(self.value)


@MongoWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        return "%s.%s" % (self.subject.to_mongo(contexts), self.attr_name)


@MongoWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(wrappers.LenWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        mrq = self.subject.to_mongo(contexts)
        mrq.count = True
        return mrq


@MongoWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(wrappers.OneWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        mrq = self.subject.to_mongo(contexts)
        mrq.one = True
        return mrq


@MongoWrapper.register_wrapper(requests.AddRequest)
class AddWrapper(wrappers.AddWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        # Things that are not an add:
        subject = self.subject.to_mongo(contexts)
        other = self.other.to_mongo(contexts)
        if all(isinstance(x, types.Dict) for x in (
            self.subject.return_type(contexts),
            self.other.return_type(contexts))):
            if isinstance(other, MongoRequest):
                raise RageQuit(
                    self, "I don't know what to do with a sub request")

            if subject == "this":
                subject = {"this": True}
            if other == "this":
                other = {"this": True}
            merged_dict = dict(subject)
            merged_dict.update(other.items())
            return merged_dict
        return "(%s + %s)" % (subject, other)


@MongoWrapper.register_wrapper(requests.SubRequest)
class SubWrapper(wrappers.SubWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s - %s)" % (self.subject.to_mongo(contexts),
                              self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.MulRequest)
class MulWrapper(wrappers.MulWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s * %s)" % (self.subject.to_mongo(contexts),
                              self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.DivRequest)
class DivWrapper(wrappers.DivWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(%s / %s)" % (self.subject.to_mongo(contexts),
                              self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.PowRequest)
class PowWrapper(wrappers.PowWrapper, BinaryMongoWrapper):

    def to_mongo(self, contexts=()):
        return "(Math.pow(%s, %s))" % (
            self.subject.to_mongo(contexts), self.other.to_mongo(contexts))


@MongoWrapper.register_wrapper(requests.SortRequest)
class SortWrapper(wrappers.SortWrapper, ContextDefinerMongoWrapper):

    def to_mongo(self, contexts=()):
        mrq = self.subject.to_mongo(contexts)

        for key, reverse in self.sort_keys:
            key = key.to_mongo(
                self.sub(contexts)).replace(
                "this.", "value." if mrq.mapreduces else "")
            mrq.sort.append((key, -1 if reverse else 1))
        return mrq


@MongoWrapper.register_wrapper(requests.MapRequest)
class MapWrapper(wrappers.MapWrapper, ContextDefinerMongoWrapper):

    def to_mongo(self, contexts=()):
        mrq = self.subject.to_mongo(contexts)
        mapped = self.new_value.to_mongo(self.sub(contexts))
        if isinstance(mapped, basestring):
            # Anonymous map reduce
            mapped = {"____": mapped}
        mrq.mapreduces.append(make_mr_map(mapped, mrq.pop_where()))
        return mrq


@MongoWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(wrappers.DictWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        new_dict = {}
        for key, val in self.value.items():
            new_val = val.to_mongo(contexts)
            if isinstance(new_val, MongoRequest):
                raise RageQuit(
                    self, "I don't know what to do with a sub request")
            new_dict[key] = new_val
        return new_dict


@MongoWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(wrappers.ContextWrapper, MongoWrapper):

    def to_mongo(self, contexts=()):
        if not isinstance(contexts[-1], types.Dict):
            return "this.____"
        return "this"
