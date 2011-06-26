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
        if isinstance(request, cls):
            # Already wrapped
            return request
        request = requests.as_request(request)
        return cls.class_from_request_class(type(request))(request)

    def __init__(self, wrapped_request):
        self.wrapped_request = wrapped_request

    # Default to getting attributes in the wrapped request, but not on
    # setting: setting puts the new value in the wrapper, which may hide
    # an attribute in the wrapped request.
    def __getattr__(self, name):
        return object.__getattribute__(self.wrapped_request, name)

    def return_type(self, contexts=()):
        """Contexts is a tuple representing the stack of types accessible via context(index)"""
        raise NotImplementedError("return_type is not implemented")

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.wrapped_request)

    def used_types(self, contexts=()):
        return {self.return_type(contexts): set((self,))}

    def merge_dict(self, left, right):
        for type, expr in right.iteritems():
            exprs = left.setdefault(type, set())
            exprs.add(self)
            left[type] = exprs | expr


@RequestWrapper.register_wrapper(requests.StoredItemsRequest)
class StoredItemsWrapper(RequestWrapper):
    def return_type(self, contexts=()):
        return self.storage.sequence_type

    def used_types(self, contexts=()):
        return {self.return_type(contexts).inner_type: set((self,))}



@RequestWrapper.register_wrapper(requests.LiteralRequest)
class LiteralWrapper(RequestWrapper):
    def return_type(self, contexts=()):
        return Type(type=type(self.value))



@RequestWrapper.register_wrapper(requests.ListRequest)
class ListWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(ListWrapper, self).__init__(*args, **kwargs)
        self.value = [self.from_request(r) for r in self.value]

    def return_type(self, contexts=()):
        if self.value:
            inner_type = self.value[0].return_type(contexts)
            if all((value.return_type(contexts) == inner_type
                for value in self.value)):
                return List(inner_type=inner_type)
        return List(inner_type=Type(type=object))

    def used_types(self, contexts=()):
        types = {}
        for value in self.value:
            used_types = value.used_types(contexts)
            self.merge_dict(types, used_types)
        return types



@RequestWrapper.register_wrapper(requests.TupleRequest)
class TupleWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(TupleWrapper, self).__init__(*args, **kwargs)
        self.value = tuple(self.from_request(r) for r in self.value)

    def return_type(self, contexts=()):
        return Type(type=tuple)

    def used_types(self, contexts=()):
        types = {}
        for value in self.value:
            used_types = value.used_types(contexts)
            self.merge_dict(types, used_types)
        return types


@RequestWrapper.register_wrapper(requests.DictRequest)
class DictWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(DictWrapper, self).__init__(*args, **kwargs)
        self.value = dict(
            (key, self.from_request(value))
            for key, value in self.value.iteritems())

    def return_type(self, contexts=()):
        mapping = {}
        for key, request in self.value.iteritems():
            mapping[key] = request.return_type(contexts)
        return Dict(mapping=mapping)

    def used_types(self, contexts=()):
        types = {}
        for value in self.value.values():
            used_types = value.used_types(contexts)
            self.merge_dict(types, used_types)
        return types



@RequestWrapper.register_wrapper(requests.ContextRequest)
class ContextWrapper(RequestWrapper):
    def return_type(self, contexts=()):
        return contexts[self.scope_depth - 1]


@RequestWrapper.register_wrapper(requests.OperationRequest)
class OperationWrapper(RequestWrapper):
    def __init__(self, *args, **kwargs):
        super(OperationWrapper, self).__init__(*args, **kwargs)
        self.subject = self.from_request(self.subject)

    def return_type(self, contexts=()):
        return self.subject.return_type(contexts)

    def used_types(self, contexts=()):
        return self.subject.used_types(contexts)

# UnaryOperationRequest has nothing more than OperationWrapper


@RequestWrapper.register_wrapper(requests.BinaryOperationRequest)
class BinaryOperationWrapper(OperationWrapper):
    def __init__(self, *args, **kwargs):
        super(BinaryOperationWrapper, self).__init__(*args, **kwargs)
        self.other = self.from_request(self.other)

    def used_types(self, contexts=()):
        types = {}
        for other in (self.subject, self.other):
            self.merge_dict(types, other.used_types(contexts))
        return types




@RequestWrapper.register_wrapper(requests.AttributeRequest)
class AttributeWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        initial_types = self.subject.return_type(contexts)
        if isinstance(initial_types, Dict):
            return initial_types.mapping[self.attr_name]
        else:
            # If the subject is not an item-like, can't infer anything
            return Type(type=object)

    def used_types(self, contexts=()):
        return {self.return_type(contexts): set((self,))}

@RequestWrapper.register_wrapper(requests.BinaryOperationRequest)
class BooleanOperationWrapper(BinaryOperationWrapper):
    def return_type(self, contexts=()):
        return Type(type=bool)


# Only the binary ones, exclude invert.
BOOL_OPERATORS = ('and', 'or', 'eq', 'ne', 'lt', 'gt', 'le', 'ge', 'regex')


def defclass(operator, base_class):
    class_name = operator.title() + 'Wrapper'
    request_class = getattr(requests, operator.title() + 'Request')
    class_ = type(class_name, (base_class,), {})
    class_ = RequestWrapper.register_wrapper(request_class)(class_)
    setattr(sys.modules[__name__], class_name, class_)

for operator in BOOL_OPERATORS:
    defclass(operator, BooleanOperationWrapper)


@RequestWrapper.register_wrapper(requests.NotRequest)
class NotOperationWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        return Type(type=bool)


@RequestWrapper.register_wrapper(requests.StrRequest)
class StrWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        return Type(type==unicode)


@RequestWrapper.register_wrapper(requests.LowerRequest)
class LowerWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        return Type(type==unicode)


@RequestWrapper.register_wrapper(requests.UpperRequest)
class UpperWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        return Type(type==unicode)




ARITHMETIC_OPERATORS = ('sub', 'mul', 'div', 'pow')


class ArithmeticOperationWrapper(BinaryOperationWrapper):
    def return_type(self, contexts=()):
        left_type = self.subject.return_type(contexts)
        right_type = self.other.return_type(contexts)
        if left_type == right_type and left_type.corn:
            return Type(type=left_type.type)
        return left_type.common_type(right_type)

for operator in ARITHMETIC_OPERATORS:
    defclass(operator, ArithmeticOperationWrapper)

@RequestWrapper.register_wrapper(requests.AddRequest)
class AddWrapper(ArithmeticOperationWrapper):
    def return_type(self, contexts=()):
        """
        Add is special, because it can work with Dict, and is not an addition
        anymore.
        """
        left_type = self.subject.return_type(contexts)
        right_type = self.other.return_type(contexts)
        if isinstance(left_type, Dict) and isinstance(right_type, Dict):
            mapping = left_type.mapping.copy()
            mapping.update(right_type.mapping)
            return Dict(mapping=mapping)
        else:
            return super(AddWrapper, self).return_type(contexts)



@RequestWrapper.register_wrapper(requests.DistinctRequest)
@RequestWrapper.register_wrapper(requests.SliceRequest)
class PreservingWrapper(OperationWrapper):
    """
    Return type is the same as the operation’s subject.
    """
    def return_type(self, contexts=()):
        return self.subject.return_type(contexts)


@RequestWrapper.register_wrapper(requests.FilterRequest)
class FilterWrapper(PreservingWrapper):
    def __init__(self, *args, **kwargs):
        super(FilterWrapper, self).__init__(*args, **kwargs)
        self.predicate = self.from_request(self.predicate)

    def used_types(self, contexts=()):
        types = super(FilterWrapper, self).used_types(contexts)
        self.merge_dict(types, self.predicate.used_types(contexts +
            (self.subject.return_type(contexts).inner_type,)))
        return types


@RequestWrapper.register_wrapper(requests.MapRequest)
class MapWrapper(OperationWrapper):
    def __init__(self, *args, **kwargs):
        super(MapWrapper, self).__init__(*args, **kwargs)
        self.new_value = self.from_request(self.new_value)

    def return_type(self, contexts=()):
        newcontext = self.subject.return_type(contexts).inner_type
        return List(
            inner_type=self.new_value.return_type(contexts + (newcontext,)))

    def used_types(self, contexts=()):
        newcontext = contexts + (self.subject.return_type(contexts).inner_type,)
        types = self.subject.used_types(contexts)
        self.merge_dict(types, self.new_value.used_types(newcontext))
        return types



@RequestWrapper.register_wrapper(requests.GroupbyRequest)
class GroupbyWrapper(OperationWrapper):
    def __init__(self, *args, **kwargs):
        super(GroupbyWrapper, self).__init__(*args, **kwargs)
        self.key = self.from_request(self.key)
        self.aggregates = self.from_request(self.aggregates)

    def return_type(self, contexts=()):
        subject_type = self.subject.return_type(contexts)
        key_type = self.key.return_type(contexts + (subject_type.inner_type,))

        aggregates_types = dict(self.aggregates.return_type(
            contexts + (subject_type,)).mapping)
        aggregates_types['key'] = key_type
        return List(inner_type=Dict(mapping=aggregates_types))

    def used_types(self, contexts=()):
        subject_type = self.subject.return_type(contexts)
        types = self.subject.used_types(contexts)
        self.merge_dict(types, self.key.used_types(contexts + (subject_type.inner_type,)))
        self.merge_dict(types, self.aggregates.used_types(contexts + (subject_type,)))
        return types



@RequestWrapper.register_wrapper(requests.LenRequest)
class LenWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        return Type(type=int)


@RequestWrapper.register_wrapper(requests.SortRequest)
class SortWrapper(PreservingWrapper):
    def __init__(self, *args, **kwargs):
        super(SortWrapper, self).__init__(*args, **kwargs)
        self.sort_keys = tuple(
            (self.from_request(sort_key), reverse)
            for sort_key, reverse in self.sort_keys)


@RequestWrapper.register_wrapper(requests.MaxRequest)
@RequestWrapper.register_wrapper(requests.MinRequest)
@RequestWrapper.register_wrapper(requests.SumRequest)
@RequestWrapper.register_wrapper(requests.IndexRequest)
class AggregateWrapper(OperationWrapper):
    def return_type(self, contexts=()):
        subject_type = self.subject.return_type(contexts)
        if isinstance(subject_type, List):
            return subject_type.inner_type
        return Type(type == object)


@RequestWrapper.register_wrapper(requests.OneRequest)
class OneWrapper(AggregateWrapper):
    def __init__(self, *args, **kwargs):
        super(OneWrapper, self).__init__(*args, **kwargs)
        if self.default is not None:
            self.default = self.from_request(self.default)

    def return_type(self, contexts=()):
        self.subject_type = super(OneWrapper, self).return_type(contexts)
        if self.default:
            return self.default.return_type(contexts).common_type(
                    self.subject_type)
        else:
            return self.subject_type
