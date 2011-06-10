# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
from . import requests
from .types import Type, Dict, List


class RequestWrapper(object):
    class_map = {}

    @classmethod
    def register_wrapper(cls, wrapped_class):
        def decorator(wrapper_class):
            cls.class_map[wrapped_class] = wrapper_class
            return wrapper_class
        return decorator

    @classmethod
    def class_from_request_class(cls, request_class):
        for class_ in request_class.mro():
            wrapper_class = cls.class_map.get(class_, None)
            if wrapper_class is not None:
                return wrapper_class
        raise TypeError('No request wrapper for type %s.' % request_class)

    @classmethod
    def from_request(cls, request):
        return cls.class_from_request_class(type(request))(request)

    def __init__(self, wrapped_request):
        self.wrapped_request = wrapped_request
        self.args = wrapped_request._Request__args

    def return_type(self, contexts=()):
        raise NotImplementedError("return_type is not implemented")
    
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.wrapped_request)


@RequestWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(StoredItemsWrapper, self).__init__(*args, **kwargs)
        self.storage, = self.args

    def return_type(self, context=()):
        return List(inner_type=Dict(
            mapping=self.storage.properties, corn=self.storage))


@RequestWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(LiteralWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args

    def return_type(self, contexts=()):
        return Type(type=type(self.value))


@RequestWrapper.register_wrapper(requests.ListRequest)
class ListWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(ListWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = [self.from_request(r) for r in self.value]

    def return_type(self, contexts=()):
        return Type(type=list)



@RequestWrapper.register_wrapper(requests.TupleRequest)
class TupleWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(TupleWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = tuple(self.from_request(r) for r in self.value)

    def return_type(self, contexts=()):
        return Type(type=tuple)



@RequestWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(DictWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = dict(
            (key, self.from_request(value))
            for key, value in self.value.iteritems())

    def return_type(self, contexts=()):
        return Type(type=dict)



@RequestWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(ContextWrapper, self).__init__(*args, **kwargs)
        self.scope_depth, = self.args

    def return_type(self, context=()):
        return context[self.scope_depth - 1].return_type(context[:-1])


@RequestWrapper.register_wrapper(requests.OperationRequest)
class OperationWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(OperationWrapper, self).__init__(*args, **kwargs)

        request_class = type(self.wrapped_request)
        self.method_name = requests.METHOD_NAME_BY_OPERATION_CLASS.get(
            request_class, None)
        self.operator_name = requests.OPERATOR_NAME_BY_OPERATION_CLASS.get(
            request_class, None)
            
        assert (self.method_name and not self.operator_name) or (
                self.operator_name and not self.method_name)

        self.subject = self.from_request(self.args[0])
        self.args = (self.subject,) + self.args[1:]
        self._init_other_args()

    def _init_other_args(self):
        self.args = (self.subject,) + tuple(
            self.from_request(r) for r in self.args[1:])


@RequestWrapper.register_wrapper(requests.GetattrRequest)
class GetattrWrapper(OperationWrapper):
    def _init_other_args(self):
        subject, attr_name = self.args
        self.attr_name = attr_name # supposed to be str or unicode

    def return_type(self, contexts=()):
        initial_types = self.subject.return_type(contexts)
        if isinstance(initial_types, Dict):
            return initial_types.mapping[self.attr_name]
        else:
            # If the subject is not an item-like, can't infer anything
            return Type(type=object)

class BooleanOperationWrapper(OperationWrapper):

    def return_type(self, contexts=()):
        return Type(type=bool)

BOOL_OPERATORS = ('and', 'or', 'xor', 'contains', 'eq', 'ne',
    'lt', 'gt', 'le', 'ge')

def defclass(operator, base_class):
    class_name = operator.title() + 'Wrapper'
    request_class = getattr(requests, operator.title() + 'Request')
    class_ = type(class_name, (base_class,), {})
    class_ = RequestWrapper.register_wrapper(request_class)
    setattr(sys.modules[__name__], class_name, class_)



for operator in BOOL_OPERATORS:
   defclass(operator, BooleanOperationWrapper)

ARITHMETIC_OPERATORS = ('add', 'sub', 'mul', 'floordiv', 'div', 'truediv', 'pow', 'mod')

class ArithmeticOperationWrapper(OperationWrapper):

    def return_type(self, contexts=()):
        left_type = self.args[0].return_type(contexts)
        right_type = self.args[0].return_type(contexts)
        if left_type.type == right_type.type:
            return Type(type=left_type.type)
        else:
            # TODO: refine type inference for heterogeneous types
            return Type(type=object)


for operator in BOOL_OPERATORS:
   defclass(operator, ArithmeticOperationWrapper)

@RequestWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(RequestWrapper):

    def __init__(self, *args, **kwargs):
        super(FilterWrapper, self)
        self.subject = self.args[0]
        self.predicate = self.args[1]

    def return_type(self, contexts=()):
        # A filter does not modify its subject
        # assert self.predicate.return_type().type == bool
        return self.subject.return_type(contexts + (self.subject,))


@RequestWrapper.register_wrapper(requests.MapRequest)
class MapWrapper(RequestWrapper):

    def __init__(self, *args, **kwargs):
        super(MapWrapper, self).__init__(self, *args, **kwargs)
        self.suject = self.args[0]
        self.operation = self.args[1]

    def return_type(self, contexts=()):
        return self.operation.return_type(contexts + (self.subject,))


@RequestWrapper.register_wrapper(requests.SortRequest)
class GroupbyWrapper(RequestWrapper):

    def __init__(self, *args, **kwargs):
        super(GroupbyWrapper, self).__init__(self, *args, **kwargs)
        self.subject = self.args[0]

    def return_type(self, contexts=()):
        subject_type = self.subject.return_type(contexts)
        key_type = self.key.return_type(contexts + (self.subject,))
        return Dict(mapping={'grouper': subject_type,
            'elements': List(inner_type=key_type)})


class PreservingWrapper(RequestWrapper):

    def __init__(self, *args, **kwargs):
        super(PreservingWrapper, self).__init__(*args, **kwargs)
        self.subject = self.args[0]

    def return_type(self, contexts=()):
        return self.suject.return_type(contexts)


@RequestWrapper.register_wrapper(requests.DistinctRequest)
class DistinctWrapper(PreservingWrapper):
    pass


@RequestWrapper.register_wrapper(requests.SortRequest)
class SortWrapper(PreservingWrapper):
    pass

@RequestWrapper.register_wrapper(requests.MaxRequest)
class MaxWrapper(PreservingWrapper):
    pass

@RequestWrapper.register_wrapper(requests.MinRequest)
class MinWrapper(PreservingWrapper):
    pass

@RequestWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(RequestWrapper):

    def return_type(self, contexts=()):
        return Type(type=int)

@RequestWrapper.register_wrapper(requests.SumRequest)
class SumWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(SumWrapper, self).__init__(*args, **kwargs)
        self.subject = self.args[0]

    def return_type(self, contexts=()):
        subject_type = self.subject.return_type(contexts)
        if isinstance(subject_type, List):
            # Is sum supposed to keep the type ?
            return subject_type.inner_type
        else:
            # If this really supposed to happen?
            return Type(type=object)


@RequestWrapper.register_wrapper(requests.GetitemRequest)
class GetitemWrapper(OperationWrapper):
    def _init_other_args(self):
        subject, key = self.args
        self.attr_name = key # int, slice, string, ...


@RequestWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(OperationWrapper):
    def _init_other_args(self):
        subject, default = self.args
        if default is not None:
            default = self.from_request(default)
        self.default = default

    def return_type(self, context=()):
        subject_type = self.subject.return_type(context)
        if isinstance(subject_type, List):
            return subject_type.inner_type
        else:
            # What should we do with something not a list ?
            # Probably an object !
            return Type(type=object)
