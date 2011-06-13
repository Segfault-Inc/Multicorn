# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
import operator

from .requests import requests
from .requests import wrappers



class PythonExecutor(wrappers.RequestWrapper):
    class_map = {}


@PythonExecutor.register_wrapper(requests.StoredItemsRequest)
class StoredItemExecutor(PythonExecutor, wrappers.StoredItemsWrapper):
    def execute(self, contexts):
        return self.storage._all()


@PythonExecutor.register_wrapper(requests.LiteralRequest)
class LiteralExecutor(PythonExecutor, wrappers.LiteralWrapper):
    def execute(self, contexts):
        return self.value


@PythonExecutor.register_wrapper(requests.ListRequest)
class ListExecutor(PythonExecutor, wrappers.ListWrapper):
    def execute(self, contexts):
        return [element.execute(contexts) for element in self.value]


@PythonExecutor.register_wrapper(requests.TupleRequest)
class TupleExecutor(PythonExecutor, wrappers.TupleWrapper):
    def execute(self, contexts):
        return tuple(element.execute(contexts) for element in self.value)


@PythonExecutor.register_wrapper(requests.DictRequest)
class DictExecutor(PythonExecutor, wrappers.DictWrapper):
    def execute(self, contexts):
        return dict(
            (key, value.execute(contexts))
            for key, value in self.value.iteritems())


@PythonExecutor.register_wrapper(requests.ContextRequest)
class ContextExecutor(PythonExecutor, wrappers.ContextWrapper):
    def execute(self, contexts):
        assert self.scope_depth <= 0
        # scope_depth of 0 -> last element
        # scope_depth of -1 -> second last element
        # ...
        return contexts[self.scope_depth - 1]


@PythonExecutor.register_wrapper(requests.OperationRequest)
class OperationExecutor(PythonExecutor, wrappers.OperationWrapper):
    def execute(self, contexts):
        # TODO: message for this error
        assert self.operator_name
        args = [arg.execute(contexts) for arg in self.args]
        operator_function = getattr(operator, '__%s__' % self.operator_name)
        return operator_function(*args)


def operation_executor(request_class, include_contexts=False,
                       execute_all=False):
    def decorator(function):
        def execute(self, contexts):
            if execute_all:
                args = tuple(arg.execute(contexts) for arg in self.args)
            else:
                exectuted_subject = self.subject.execute(contexts)
                args = (exectuted_subject,) + self.other_args
            if include_contexts:
                args = (contexts,) + args
            return function(*args)
        
        name = request_class.__name__
        assert name.endswith('Request')
        name = name[:-len('Request')] # remove the Request suffix
        class_name = name + 'Executor'
        base_wrapper = wrappers.RequestWrapper.class_from_request_class(
            request_class)
        executor_class = type(class_name,
                              (OperationExecutor, base_wrapper),
                              {'execute': execute})
        setattr(sys.modules[__name__], class_name, executor_class)
        PythonExecutor.register_wrapper(request_class)(executor_class)

        return function
    return decorator


@operation_executor(requests.AttributeRequest)
def execute_getattribute(subject, attr_name):
    # XXX The execution of __getattr__ is actually __getitem__ !!
    # eg. if r represents item, r.firstname represents item['firstname']
    return subject[attr_name]


# TODO: different handling?
@operation_executor(requests.SliceRequest)
@operation_executor(requests.IndexRequest)
def execute_getitem(subject, key):
    # TODO special handling in case `subject` is a generator
    # other cases?
    try:
        return subject[key]
    except TypeError:
        # subject could be an iterable without a __getitem__ like a generator
        # TODO: implement indexing and slicing
        raise


@operation_executor(requests.SortRequest, include_contexts=True)
def execute_sort(contexts, sequence, *sort_keys):
    assert sort_keys, 'Got a sort request with no sort key.'

    all_reverse = all(key.reverse for key in sort_keys)
    all_non_reverse = all(not key.reverse for key in sort_keys)
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
        # of any ordering already present (so it should not be too inefficient).
        
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


@operation_executor(requests.MapRequest, include_contexts=True)
def execute_map(contexts, sequence, new_element):
    for element in sequence:
        yield new_element.execute(contexts + (element,))


@operation_executor(requests.FilterRequest, include_contexts=True)
def execute_filter(contexts, sequence, predicate):
    for element in sequence:
        if predicate.execute(contexts + (element,)):
            yield element


@operation_executor(requests.GroupbyRequest, include_contexts=True)
def execute_groupby(contexts, sequence, group_key):
    groups = {}
    for element in sequence:
        key = group_key.execute(contexts + (element,))
        groups.setdefault(key, []).append(element)

    for grouper, elements in groups.iteritems():
        yield {'grouper': grouper, 'elements': elements}


@operation_executor(requests.DistinctRequest)
def distinct(sequence):
    seen = set()
    for element in sequence:
        if element not in seen:
            seen.add(element)
            yield element


@operation_executor(requests.OneRequest, include_contexts=True)
def execute_one(contexts, sequence, default):
    iterator = iter(sequence)
    stop_iteration_marker = object()
    element = next(iterator, stop_iteration_marker)
    if element is stop_iteration_marker:
        if default is None:
            # TODO specific exception
            raise IndexError('.one() on an empty sequence')
        else:
            element = default.execute(contexts)
    if next(iterator, stop_iteration_marker) is stop_iteration_marker:
        return element
    else:
        # TODO specific exception
        raise ValueError('More than one element in .one()')


@operation_executor(requests.AddRequest, execute_all=True)
def execute_add(left, right):
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        result = dict(left)
        result.update(right)
        return result
    else:
        return left + right


operation_executor(requests.SumRequest)(sum)
operation_executor(requests.MinRequest)(min)
operation_executor(requests.MaxRequest)(max)
operation_executor(requests.LenRequest)(len) # TODO: handle generators

del operation_executor


def execute(request):
    return PythonExecutor.from_request(request).execute(())

