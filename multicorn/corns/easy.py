# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
import sys
from collections import Iterable
from .abstract import AbstractCorn
from ..requests.types import Type, List
from ..requests import requests, wrappers
from ..requests.helpers import cut_on_index
from .. import python_executor


class EasyFilter():

    def litteral(self, value):
        return value

    def attribute(self, value):
        return value


def easy(maybe_easy, *args):
    if hasattr(maybe_easy, 'to_easy'):
        return maybe_easy.to_easy(*args)
    raise NotImplementedError("%r not implemented" % maybe_easy)


class EasyWrapper(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()

    def to_easy(self, easy_filter):
        raise NotImplementedError()

# Only the binary ones, exclude invert.
FILTER_OPERATORS = {
    'and': 'and_',
    'or': 'or_',
    'eq': 'equal',
    'ne': 'not_equal',
    'lt': 'lesser_than',
    'gt': 'greater_than',
    'le': 'lesser_or_equal',
    'ge': 'greater_or_equal',
    'regex': 'matches',
    'in': 'is_in'}

for operator, filter_fun in FILTER_OPERATORS.items():
    class_name = operator.title() + 'Wrapper'
    request_class = getattr(requests, operator.title() + 'Request')
    wrapper_class = getattr(wrappers, operator.title() + 'Wrapper')
    class_ = type(class_name, (wrapper_class, EasyWrapper), {})

    def lambda_are_broken(filter_fun):
        return lambda kc, easy_filter: (
            getattr(easy_filter, filter_fun)(
                easy(kc.subject, easy_filter),
                easy(kc.other, easy_filter)))

    class_.to_easy = lambda_are_broken(filter_fun)

    class_ = EasyWrapper.register_wrapper(request_class)(class_)
    setattr(sys.modules[__name__], class_name, class_)

    def lambda_are_broken(filter_fun):

        def not_implemented(slf, op1, op2):
            raise NotImplementedError("%s not implemented" % filter_fun)
        return not_implemented

    setattr(EasyFilter, filter_fun, lambda_are_broken(filter_fun))


@EasyWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(wrappers.StoredItemsWrapper, EasyWrapper):

    def to_easy(self, easy_filter):
        pass


@EasyWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(wrappers.FilterWrapper, EasyWrapper):

    def to_easy(self, easy_filter):
        return easy(self.predicate, easy_filter)


@EasyWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(wrappers.LiteralWrapper, EasyWrapper):

    def to_easy(self, easy_filter):
        return easy_filter.litteral(self.value)


@EasyWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(wrappers.AttributeWrapper, EasyWrapper):

    def to_easy(self, easy_filter):
        return easy_filter.attribute(self.attr_name)


class EasyCorn(AbstractCorn):
    """
    This is an helper for creating corns with simple optimizations.
    """

    def register(self, name, type=unicode):
        type = Type(corn=self, name=name, type=type)
        self.properties[name] = type

    def execute(self, request):
        chain = requests.as_chain(request)
        if (self.EasyFilter and
            len(chain) > 1 and
            isinstance(chain[1], requests.FilterRequest)):
            filter, other = cut_on_index(request, 1)
            easy_wrapped_filter = EasyWrapper.from_request(filter)
            try:
                results = self.filter(easy_wrapped_filter
                                          .to_easy(self.EasyFilter()))

                return self.RequestWrapper.from_request(
                        other).execute((results,))

            except NotImplementedError as e:
                self.log.warning("Not implemented %s" % e)
                return python_executor.execute(request)

        self.log.warning("%r not optimized" % request)
        return python_executor.execute(request)
