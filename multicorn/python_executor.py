# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

import sys
import operator
from itertools import islice, chain
from collections import Mapping, Iterable, deque

from .requests import requests
from .requests import wrappers
from .requests.helpers import inject_context


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
    return [execute(element, contexts) for element in self.value]


@register_executor(requests.TupleRequest)
def execute_tuple(self, contexts):
    return tuple(execute(element, contexts) for element in self.value)


@register_executor(requests.DictRequest)
def execute_dict(self, contexts):
    return dict(
        (key, execute(value, contexts))
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
    left = execute(self.subject, contexts)
    right = execute(self.other, contexts)
    operator_function = getattr(operator, '__%s__' % self.operator_name)
    return operator_function(left, right)


@register_executor(requests.UnaryOperationRequest)
def execute_unary_operation(self, contexts):
    # TODO: message for this error
    assert self.operator_name
    subject = execute(self.subject, contexts)
    operator_function = getattr(operator, '__%s__' % self.operator_name)
    return operator_function(subject)


@register_executor(requests.AttributeRequest)
def execute_attribute(self, contexts):
    subject = execute(self.subject, contexts)
    # XXX The execution of __getattr__ is actually __getitem__ !!
    # eg. if r represents item, r.firstname represents item['firstname']
    return subject[self.attr_name]


@register_executor(requests.SliceRequest)
def execute_slice(self, contexts):
    subject = execute(self.subject, contexts)
    try:
        return subject[self.slice]
    except TypeError:
        # subject could be an iterable but not a sequence, eg. a generator
        # XXX this consumes the iterable. This may be unexpected if a
        # generator is used more than once.
        start = self.slice.start
        stop = self.slice.stop
        step = self.slice.step

        if start is None:
            start = 0
        if step is None:
            step = 1
        if step == 0:
            raise ValueError('Step can not be zero for slicing.')

        if start >= 0 and (stop is None or stop >= 0) and step > 0:
            return islice(subject, start, stop, step)
        elif start >= 0 and (stop is None or stop >= 0):
            # step < 0  =>  step > 0
            step = -step
            return reversed(list(islice(subject, start, stop, step)))
        else:
            return list(subject)[self.slice]


@register_executor(requests.IndexRequest)
def execute_index(self, contexts):
    subject = execute(self.subject, contexts)
    try:
        return subject[self.index]
    except TypeError:
        # subject could be an iterable but not a sequence, eg. a generator
        # XXX this consumes the iterable. This may be unexpected if a
        # generator is used more than once.
        if self.index >= 0:
            iterator = iter(subject)
            try:
                for i in xrange(self.index):
                    # Conusme previous elements.
                    next(iterator)
                return next(iterator)
            except StopIteration:
                raise IndexError('Index %i is out of range.' % self.index)
        else:
            # self.index < 0
            index_from_end = -self.index
            queue = deque(subject, maxlen=index_from_end)
            if len(queue) < index_from_end:
                raise IndexError('Index %i is out of range.' % self.index)
            return queue[0]


@register_executor(requests.SortRequest)
def execute_sort(self, contexts):
    sequence = execute(self.subject, contexts)
    sort_keys = self.sort_keys
    assert sort_keys, 'Got a sort request with no sort key.'

    all_reverse = all(reverse for key, reverse in sort_keys)
    all_non_reverse = all(not reverse for key, reverse in sort_keys)
    if all_reverse or all_non_reverse:
        # The sort direction is the same for all keys: we can use a single
        # key function and a single `reverse` argument.
        def key_function(element):
            return tuple(execute(key, contexts + (element,))
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
            return execute(key_request, contexts + (element,))

        sequence = list(sequence)
        # sort_keys is in most-significant key fist, we want to do successive
        # sorts least-significant fist.
        for key_request, reverse in reversed(sort_keys):
            sequence.sort(key=key_function, reverse=reverse)

        return sequence


@register_executor(requests.MapRequest)
def execute_map(self, contexts):
    sequence = execute(self.subject, contexts)
    for element in sequence:
        yield execute(self.new_value, contexts + (element,))


@register_executor(requests.FilterRequest)
def execute_filter(self, contexts):
    sequence = execute(self.subject, contexts)
    for element in sequence:
        if execute(self.predicate, contexts + (element,)):
            yield element


@register_executor(requests.GroupbyRequest)
def execute_groupby(self, contexts):
    sequence = execute(self.subject, contexts)
    iterator = iter(sequence)
    groups = {}
    for element in iterator:
        key = execute(self.key, contexts + (element,))
        try:
            elements = groups.setdefault(key, [])
        except TypeError:
            # key is not hashable
            break
        elements.append(element)
    else:
        for key, group in groups.iteritems():
            yield {'key': key,
                   'group': execute(self.aggregate, contexts + (group,))}
        return

    # We have at least one non-hashable key/grouper, we can not use a dict
    # anymore.
    groups = groups.items() # as a list.
    # We got here on a non-hashable key, so it can not be in groups yet.
    groups.append((key, [element]))

    # Continue the same iterator
    for element in iterator:
        key = execute(self.key, contexts + (element,))
        # Search the list for this key/grouper.
        for grouper, elements in groups:
            if grouper == key:
                break
        else:
            elements = []
            groups.append((key, elements))
        elements.append(element)

    for key, group in groups:
        yield {'key': key,
              'group': execute(self.aggregate, contexts + (group,))}


@register_executor(requests.DistinctRequest)
def execute_distinct(self, contexts):
    sequence = execute(self.subject, contexts)
    iterator = iter(sequence)

    seen = set()
    for element in iterator:
        try:
            new = element not in seen
        except TypeError:
            # element is not hashable
            break
        if new:
            seen.add(element)
            yield element
    else:
        # We got to the end of the iterator without breaking
        return

    # At least one element was not hashable, fall back on a list
    # instead of a set. `in` is slower, but the results should be the same
    seen = list(seen)
    seen.append(element)
    yield element
    # Continue with the non hashable lemenent same iterator
    for element in iterator:
        if element not in seen:
            seen.append(element)
            yield element


@register_executor(requests.OneRequest)
def execute_one(self, contexts):
    sequence = execute(self.subject, contexts)
    iterator = iter(sequence)
    stop_iteration_marker = object()
    element = next(iterator, stop_iteration_marker)
    if element is stop_iteration_marker:
        if self.default is None:
            # TODO specific exception
            raise IndexError('.one() on an empty sequence')
        else:
            element = execute(self.default, contexts)
    if next(iterator, stop_iteration_marker) is stop_iteration_marker:
        return element
    else:
        # TODO specific exception
        raise ValueError('More than one element in .one()')


@register_executor(requests.AddRequest)
def execute_add(self, contexts):
    left = execute(self.subject, contexts)
    right = execute(self.other, contexts)
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        result = dict(left)
        result.update(right)
        return result
    if (isinstance(left, Iterable) and isinstance(right, Iterable)
            and not isinstance(left, (basestring, Mapping))
            and not isinstance(right, (basestring, Mapping))):
        return chain(left, right)
    else:
        return left + right


@register_executor(requests.AndRequest)
def execute_and(self, contexts):
    left = execute(self.subject, contexts)
    right = execute(self.other, contexts)
    return bool(left and right)


@register_executor(requests.OrRequest)
def execute_or(self, contexts):
    left = execute(self.subject, contexts)
    right = execute(self.other, contexts)
    return bool(left or right)


@register_executor(requests.LenRequest)
def execute_len(self, contexts):
    subject = execute(self.subject, contexts)
    try:
        return len(subject)
    except TypeError:
        # XXX this consumes the iterable. This may be unexpected if a
        # generator is used more than once.
        return sum(1 for element in subject)


def simple_executor(class_, function):
    @register_executor(class_)
    def execute_simple(self, contexts):
        subject = execute(self.subject, contexts)
        return function(subject)

simple_executor(requests.NotRequest, operator.not_)
simple_executor(requests.SumRequest, sum)
simple_executor(requests.MinRequest, min)
simple_executor(requests.MaxRequest, max)

del register_executor, simple_executor


def execute(request, contexts=()):
    request = PythonExecutor.from_request(request)
    if contexts:
        chain = requests.as_chain(request.wrapped_request)
        if isinstance(chain[0], requests.StoredItemsRequest):
            #We have a real chain!
            request = inject_context(request.wrapped_request, contexts)
            return requests.WithRealAttributes(chain[0]).storage.execute(request)
    return request.execute(contexts)
