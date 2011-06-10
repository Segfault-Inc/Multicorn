# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
import operator

from .requests import requests
from .requests import wrappers



class PythonExecutor(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()


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


@PythonExecutor.register_wrapper(requests.RootRequest)
class RootExecutor(PythonExecutor, wrappers.RootWrapper):
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
        assert self.operator_name and not self.method_name

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
                subject = self.args[0]
                other_args = self.args[1:]
                exectuted_subject = subject.execute(contexts)
                args = (exectuted_subject,) + other_args
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


@operation_executor(requests.GetattrRequest)
def execute_getattr(subject, attr_name):
    # XXX The execution of __getattr__ is actually __getitem__ !!
    # eg. if r represents item, r.firstname represents item['firstname']
    return subject.execute(contexts)[attr_name]


@operation_executor(requests.GetitemRequest)
def execute_getitem(subject, key):
    # TODO special handling in case `exectuted_subject` is a generator
    # other cases?
    return exectuted_subject[key]


@operation_executor(requests.SortRequest, include_contexts=True)
def execute_sort(contexts, sequence, sort_key):
    def key_function(element):
        return sort_key.execute(contexts + (element,))
    return sorted(sequence, key=key_function)


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

