# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
import operator
from collections import Mapping

from .requests import requests
from .requests import wrappers


class PythonExecutor(wrappers.RequestWrapper):
    class_map = {}


def register_executor(request_class):
    def decorator(function):
        name = request_class.__name__
        assert name.endswith('Request')
        name = name[:-len('Request')]  # remove the Request suffix
        assert name
        class_name = name + 'Executor'
        base_wrapper = wrappers.RequestWrapper.class_from_request_class(
            request_class)
        executor_class = type(class_name,
                              (PythonExecutor, base_wrapper),
                              {'execute': function})
        PythonExecutor.register_wrapper(request_class)(executor_class)

        return function
    return decorator


@register_executor(requests.StoredItemsRequest)
def execute_storage(self, contexts):
    return self.storage._all()


@register_executor(requests.LiteralRequest)
def execute_literal(self, contexts):
    return self.value


@register_executor(requests.ListRequest)
def execute_list(self, contexts):
    return [element.execute(contexts) for element in self.value]


@register_executor(requests.TupleRequest)
def execute_tuple(self, contexts):
    return tuple(element.execute(contexts) for element in self.value)


@register_executor(requests.DictRequest)
def execute_dict(self, contexts):
    return dict(
        (key, value.execute(contexts))
        for key, value in self.value.iteritems())


@register_executor(requests.ContextRequest)
def execute_context(self, contexts):
    assert self.scope_depth <= 0
    # scope_depth of 0 -> last element
    # scope_depth of -1 -> second last element
    # ...
    return contexts[self.scope_depth - 1]


@register_executor(requests.BinaryOperationRequest)
def execute_binary_operation(self, contexts):
    # TODO: message for this error
    assert self.operator_name
    left = self.subject.execute(contexts)
    right = self.other.execute(contexts)
    operator_function = getattr(operator, '__%s__' % self.operator_name)
    return operator_function(left, right)


@register_executor(requests.UnaryOperationRequest)
def execute_unary_operation(self, contexts):
    # TODO: message for this error
    assert self.operator_name
    subject = self.subject.execute(contexts)
    operator_function = getattr(operator, '__%s__' % self.operator_name)
    return operator_function(subject)


@register_executor(requests.AttributeRequest)
def execute_attribute(self, contexts):
    subject = self.subject.execute(contexts)
    # XXX The execution of __getattr__ is actually __getitem__ !!
    # eg. if r represents item, r.firstname represents item['firstname']
    return subject[self.attr_name]


@register_executor(requests.SliceRequest)
def execute_slice(self, contexts):
    subject = self.subject.execute(contexts)
    try:
        return subject[self.slice]
    except TypeError:
        # subject could be an iterable without a __getitem__ like a generator
        # TODO: implement indexing and slicing
        raise

@register_executor(requests.IndexRequest)
def execute_index(self, contexts):
    subject = self.subject.execute(contexts)
    try:
        return subject[self.index]
    except TypeError:
        # subject could be an iterable without a __getitem__ like a generator
        # TODO: implement indexing and slicing
        raise


@register_executor(requests.SortRequest)
def execute_sort(self, contexts):
    sequence = self.subject.execute(contexts)
    sort_keys = self.sort_keys
    assert sort_keys, 'Got a sort request with no sort key.'

    all_reverse = all(reverse for key, reverse in sort_keys)
    all_non_reverse = all(not reverse for key, reverse in sort_keys)
    if all_reverse or all_non_reverse:
        # The sort direction is the same for all keys: we can use a single
        # key function and a single `reverse` argument.
        def key_function(element):
            return tuple(key.execute(contexts + (element,))
                         for key, reverse in sort_keys)
        return sorted(sequence, key=key_function, reverse=all_reverse)
    else:
        # Mixed sort direction : the obvious key function can not do this.

        # http://wiki.python.org/moin/HowTo/Sorting/#Sort_Stability_and_Complex_Sorts
        # According to this page, sorts are guaranteed to be stable (so the
        # following is correct) and the Timsort algorithm used takes advantage
        # of any ordering already present
        # (so it should not be too inefficient).

        # TODO: benchmark this vs other solutions like in git 005a2d6:
        # Make a ComparingKey class and use it as a key function. (Inspired
        # by functools.cmp_to_key)

        def key_function(element):
            # key_request is referenced in the outer scope, and will change
            # for each iteration of the for-loop below.
            # No need to re-define the same function for each iteration.
            return key_request.execute(contexts + (element,))

        sequence = list(sequence)
        # sort_keys is in most-significant key fist, we want to do successive
        # sorts least-significant fist.
        for key_request, reverse in reversed(sort_keys):
            sequence.sort(key=key_function, reverse=reverse)

        return sequence


@register_executor(requests.MapRequest)
def execute_map(self, contexts):
    sequence = self.subject.execute(contexts)
    for element in sequence:
        yield self.new_value.execute(contexts + (element,))


@register_executor(requests.FilterRequest)
def execute_filter(self, contexts):
    sequence = self.subject.execute(contexts)
    for element in sequence:
        if self.predicate.execute(contexts + (element,)):
            yield element


@register_executor(requests.GroupbyRequest)
def execute_groupby(self, contexts):
    sequence = self.subject.execute(contexts)
    groups = {}
    for element in sequence:
        key = self.key.execute(contexts + (element,))
        groups.setdefault(key, []).append(element)

    for grouper, elements in groups.iteritems():
        yield {'grouper': grouper, 'elements': elements}


@register_executor(requests.DistinctRequest)
def distinct(self, contexts):
    sequence = self.subject.execute(contexts)
    seen = set()
    for element in sequence:
        if element not in seen:
            seen.add(element)
            yield element


@register_executor(requests.OneRequest)
def execute_one(self, contexts):
    sequence = self.subject.execute(contexts)
    iterator = iter(sequence)
    stop_iteration_marker = object()
    element = next(iterator, stop_iteration_marker)
    if element is stop_iteration_marker:
        if self.default is None:
            # TODO specific exception
            raise IndexError('.one() on an empty sequence')
        else:
            element = self.default.execute(contexts)
    if next(iterator, stop_iteration_marker) is stop_iteration_marker:
        return element
    else:
        # TODO specific exception
        raise ValueError('More than one element in .one()')


@register_executor(requests.AddRequest)
def execute_add(self, contexts):
    left = self.subject.execute(contexts)
    right = self.other.execute(contexts)
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        result = dict(left)
        result.update(right)
        return result
    else:
        return left + right


@register_executor(requests.AndRequest)
def execute_and(self, contexts):
    left = self.subject.execute(contexts)
    right = self.other.execute(contexts)
    return bool(left and right)


@register_executor(requests.OrRequest)
def execute_or(self, contexts):
    left = self.subject.execute(contexts)
    right = self.other.execute(contexts)
    return bool(left or right)


def simple_executor(class_, function):
    @register_executor(class_)
    def execute(self, contexts):
        subject = self.subject.execute(contexts)
        return function(subject)

simple_executor(requests.NotRequest, operator.not_)
simple_executor(requests.SumRequest, sum)
simple_executor(requests.MinRequest, min)
simple_executor(requests.MaxRequest, max)
simple_executor(requests.LenRequest, len)  # TODO: handle generators

del register_executor, simple_executor


def execute(request):
    return PythonExecutor.from_request(request).execute(())
