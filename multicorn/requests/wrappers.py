# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


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

    def resultingtypes(self):
        raise NotImplementedError("Resultingtypes is not implemented")



@RequestWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(StoredItemsWrapper, self).__init__(*args, **kwargs)
        self.storage, = self.args


@RequestWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(LiteralWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args

    def resultingtypes(self):
        return Type(type=type(self.value))


@RequestWrapper.register_wrapper(requests.ListRequest)
class ListWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(ListWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = [self.from_request(r) for r in self.value]

    def resultingtypes(self):
        return Type(type=list)



@RequestWrapper.register_wrapper(requests.TupleRequest)
class TupleWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(TupleWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = tuple(self.from_request(r) for r in self.value)

    def resultingtypes(self):
        return Type(type=tuple)



@RequestWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(DictWrapper, self).__init__(*args, **kwargs)
        self.value, = self.args
        self.value = dict(
            (key, self.from_request(value))
            for key, value in self.value.iteritems())

    def resultingtypes(self):
        return Type(type=dict)



@RequestWrapper.register_wrapper(requests.RootRequest)
class RootWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(RootWrapper, self).__init__(*args, **kwargs)
        self.scope_depth, = self.args


@RequestWrapper.register_wrapper(requests.OperationRequest)
class OperationWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(OperationWrapper, self).__init__(*args, **kwargs)

        request_class = type(self.wrapped_request)
        self.method_name = requests.METHOD_NAME_OPERATION_CLASS.get(
            request_class, None)
        self.operator_name = requests.OPERATOR_NAME_BY_OPERATION_CLASS.get(
            request_class, None)
        assert (self.method_name and not self.operator_name) or (
                self.operator_name and not self.method_name)

        self.subject = self.args[0] = self.from_request(self.args[0])
        self._init_other_args()

    def _init_other_args(self):
        self.args = (self.subject,) + tuple(
            self.from_request(r) for r in self.args[1:])


@RequestWrapper.register_wrapper(requests.GetattrRequest)
class GetattrWrapper(OperationWrapper):
    def _init_other_args(self):
        subject, attr_name = self.args
        self.attr_name = attr_name # supposed to be str or unicode

    def resultingtypes(self):
        initial_types = self.subject.resultingtypes()
        if isinstance(initial_types, Dict):
            return initial_types.mapping[self.key]
        else:
            #If the subject is not an item-like, can't infer anything
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
        

