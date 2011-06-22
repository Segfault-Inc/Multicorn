# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ...requests import requests, wrappers
from .request import MongoRequest


class MongoWrapper(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()

    def to_mongo(self):
        raise NotImplementedError()


@MongoWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, MongoWrapper):

    def to_mongo(self):
        return MongoRequest()


@MongoWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, MongoWrapper):

    def to_mongo(self):
        expression = self.subject.to_mongo()
        expression.set_current_where(self.predicate.to_mongo())
        return expression


@MongoWrapper.register_wrapper(requests.AndRequest)
class AndWrapper(wrappers.AndWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s && %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.OrRequest)
class OrWrapper(wrappers.OrWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s || %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.LeRequest)
class LeWrapper(wrappers.LeWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s <= %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.GeRequest)
class GeWrapper(wrappers.GeWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s >= %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.LtRequest)
class LtWrapper(wrappers.LtWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s < %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.GtRequest)
class GtWrapper(wrappers.GtWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s > %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.NeRequest)
class NeWrapper(wrappers.NeWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s != %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.EqRequest)
class EqWrapper(wrappers.BooleanOperationWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s == %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, MongoWrapper):

    def to_mongo(self):
        return "%r" % self.value


@MongoWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, MongoWrapper):

    def to_mongo(self):
        return "this.%s" % self.attr_name


@MongoWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(wrappers.LenWrapper, MongoWrapper):

    def to_mongo(self):
        expression = self.subject.to_mongo()
        expression.count = True
        return expression


@MongoWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(wrappers.OneWrapper, MongoWrapper):

    def to_mongo(self):
        expression = self.subject.to_mongo()
        expression.one = True
        return expression


@MongoWrapper.register_wrapper(requests.AddRequest)
class AddWrapper(wrappers.AddWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s + %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.SubRequest)
class SubWrapper(wrappers.SubWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s - %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.MulRequest)
class MulWrapper(wrappers.MulWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s * %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.DivRequest)
class DivWrapper(wrappers.DivWrapper, MongoWrapper):

    def to_mongo(self):
        return "(%s / %s)" % (self.subject.to_mongo(), self.other.to_mongo())


@MongoWrapper.register_wrapper(requests.PowRequest)
class PowWrapper(wrappers.PowWrapper, MongoWrapper):

    def to_mongo(self):
        return "(Math.pow(%s, %s))" % (
            self.subject.to_mongo(), self.other.to_mongo())
