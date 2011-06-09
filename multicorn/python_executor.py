# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
import operator

from .requests import requests
from .requests import wrappers


def add(self, other):
    """
    A `+` operator that also works on mappings:
    
        add({'a': 3, 'b': 5}, {'b': 8, 'c': 13})
         == {'a': 3, 'b': 8, 'c': 13}
        
    """
    if isinstance(self, Mapping) and isinstance(other, Mapping):
        result = dict(self)
        result.update(other)
        return result
    else:
        return self + other


class PythonExecutor(wrappers.RequestWrapper):
    class_map = wrappers.RequestWrapper.class_map.copy()


@PythonExecutor.register_wrapper(requests.Literal)
class LiteralExecutor(PythonExecutor):
    def execute(self, contexts):
        return self.value


@PythonExecutor.register_wrapper(requests.List)
class ListExecutor(PythonExecutor):
    def execute(self, contexts):
        return [element.execute(contexts) for element in self.value]


@PythonExecutor.register_wrapper(requests.Tuple)
class TupleExecutor(PythonExecutor):
    def execute(self, contexts):
        return tuple(element.execute(contexts) for element in self.value)


@PythonExecutor.register_wrapper(requests.Dict)
class DictExecutor(PythonExecutor):
    def execute(self, contexts):
        return dict(
            (key, value.execute(contexts)) 
            for key, value in self.value.iteritems())


@PythonExecutor.register_wrapper(requests.Root)
class RootExecutor(PythonExecutor):
    def execute(self, contexts):
        assert self.scope_depth <= 0
        # scope_depth of 0 -> last element
        # scope_depth of -1 -> second last element
        # ...
        return contexts[self.scope_depth - 1]


@PythonExecutor.register_wrapper(requests.Operation)
class OperationExecutor(PythonExecutor):
    def execute(self, contexts):
        # TODO: message for this error
        assert self.operator_name and not self.method_name

        args = [arg.execute(contexts) for arg in self.args]
        operator_function = getattr(operator, '__%s__' % self.operator_name)
        return operator_function(*args)


def operation_executor(request_class, include_contexts=False):
    def decorator(function):
        def execute(self, contexts):
            subject = self.args[0]
            other_args = self.args[1:]
            exectuted_subject = subject.execute(contexts)
            if include_contexts:
                return function(contexts, exectuted_subject, *other_args)
            else:
                return function(exectuted_subject, *other_args)
        
        class_name = request_class.__name__ + 'Executor'
        executor_class = type(class_name, (PythonExecutor,),
                              {'execute': execute})
        setattr(sys.modules[__name__], class_name, executor_class)
        PythonExecutor.register_wrapper(request_class)(executor_class)

        return function
    return decorator


@operation_executor(requests.GetattrOperation)
def execute_getattr(subject, attr_name):
    # XXX The execution of __getattr__ is actually __getitem__ !!
    # eg. if r represents item, r.firstname represents item['firstname']
    return subject.execute(contexts)[attr_name]


@operation_executor(requests.GetitemOperation)
def execute_getitem(subject, key):
    # TODO special handling in case `exectuted_subject` is a generator
    # other cases?
    return exectuted_subject[key]


@operation_executor(requests.SortOperation, include_contexts=True)
def execute_sort(contexts, sequence, sort_key):
    def key_function(element):
        return sort_key.execute(contexts + (element,))
    return sorted(sequence, key=key_function)


@operation_executor(requests.MapOperation, include_contexts=True)
def execute_map(contexts, sequence, new_element):
    for element in sequence:
        yield new_element.execute(contexts + (element,))


@operation_executor(requests.FilterOperation, include_contexts=True)
def execute_filter(contexts, sequence, predicate):
    for element in sequence:
        if predicate.execute(contexts + (element,)):
            yield element


@operation_executor(requests.GroupbyOperation, include_contexts=True)
def execute_groupby(contexts, sequence, group_key):
    groups = {}
    for element in sequence:
        key = group_key.execute(contexts + (element,))
        groups.setdefault(key, []).append(element)

    for grouper, elements in groups.iteritems():
        yield {'grouper': grouper, 'elements': elements}


@operation_executor(requests.DistinctOperation)
def distinct(sequence):
    seen = set()
    for element in sequence:
        if element not in seen:
            seen.add(element)
            yield element

operation_executor(requests.SumOperation)(sum)
operation_executor(requests.MinOperation)(min)
operation_executor(requests.MaxOperation)(max)
operation_executor(requests.LenOperation)(len) # TODO: handle generators

del operation_executor

