# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook

from multicorn import Multicorn

from multicorn.corns.memory import Memory
from multicorn.requests import CONTEXT as c
from multicorn.requests.requests import FilterRequest
from multicorn.declarative import declare, Property
from multicorn.requests.wrappers import (RequestWrapper, LiteralWrapper,
        EqWrapper, OrWrapper)
from multicorn.requests.requests import (as_chain, LiteralRequest,
        LtRequest, WithRealAttributes, as_request)
from multicorn.requests.helpers import (split_predicate, isolate_values,
                                        cut_on_predicate)
from multicorn.python_executor import PythonExecutor


suite = Tests()


@suite.test
def test_simple_helpers():
    root = as_request([1, 2, 3, 4, 5])
    filter = root.filter(c > 2)
    request = filter.map(c)
    chain = as_chain(request)
    assert len(chain) == 3
    left, right = cut_on_predicate(request,
            lambda x: isinstance(x, FilterRequest),
            position=0)
    assert len(as_chain(left)) == 2
    # The right part as a len of totallen - len(left) + 1,
    # because we append a dummy context
    right_chain = as_chain(right)
    assert isinstance(right_chain[0], c.__class__)
    assert isinstance(WithRealAttributes(right_chain[1]).subject, c.__class__)
    assert len(right_chain) == 2
    left_chain = as_chain(left)
    assert left_chain[-1] == filter
    full_result = list(PythonExecutor.from_request(request).execute(()))
    first_part_result = list(PythonExecutor.from_request(request).execute(()))
    second_part_result = list(
        PythonExecutor.from_request(request).execute((first_part_result,)))
    assert full_result == second_part_result


def make_corn():
    mc = Multicorn()

    @mc.register
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


@suite.test
def test_split_predicate():
    Corn = make_corn()
    filter = RequestWrapper.from_request(Corn.all.filter(c.name == 'foo'))
    # Test splitting a predicate on no property
    selfpred, other = split_predicate(filter, [])
    assert isinstance(selfpred, LiteralWrapper)
    assert selfpred.value == True
    assert other == filter.predicate
    # Test splitting a predicate on the only property
    selfpred, other = split_predicate(filter, [Corn.properties['name']])
    assert isinstance(other, LiteralWrapper)
    assert other.value == True
    assert selfpred == filter.predicate
    # Test that 'And' request are properly separated
    filter = RequestWrapper.from_request(
        Corn.all.filter((c.name == 'foo') & (c.lastname == 'bar')))
    selfpred, other = split_predicate(filter, [Corn.properties['name']])
    assert isinstance(selfpred, EqWrapper)
    assert isinstance(other, EqWrapper)
    assert selfpred.subject.attr_name == 'name'
    assert selfpred.other.value == 'foo'
    assert other.subject.attr_name == 'lastname'
    assert other.other.value == 'bar'
    # Test that 'Or' request are kept unchanged
    filter = RequestWrapper.from_request(
        Corn.all.filter((c.name == 'foo') | (c.lastname == 'bar')))
    selfpred, other = split_predicate(filter, [])
    assert isinstance(selfpred, LiteralWrapper)
    assert selfpred.value == True
    assert other == filter.predicate
    # Test that Combinations of Or and And are properly managed
    filter = RequestWrapper.from_request(
        Corn.all.filter(
            ((c.lastname == 'bar') | (c.id < 4)) & (c.name == 'foo')))
    selfpred, other = split_predicate(
        filter, [Corn.properties['id'], Corn.properties['name']])
    assert isinstance(selfpred, EqWrapper)
    assert isinstance(other, OrWrapper)
    assert selfpred.subject.attr_name == 'name'
    assert selfpred.other.value == 'foo'


@suite.test
def test_isolate_values():
    Corn = make_corn()
    filter = RequestWrapper.from_request(Corn.all.filter(c.lastname == 'foo'))
    context = (filter.subject.return_type().inner_type,)
    values, remainder = isolate_values(
        filter.predicate.wrapped_request, context)
    assert values == {'lastname': 'foo'}
    assert isinstance(remainder, LiteralRequest)
    assert RequestWrapper.from_request(remainder).value == True
    filter = RequestWrapper.from_request(
        Corn.all.filter((c.lastname == 'foo') & (c.id < 3)))
    context = (filter.subject.return_type().inner_type,)
    values, remainder = isolate_values(
        filter.predicate.wrapped_request, context)
    assert values == {'lastname': 'foo'}
    assert isinstance(remainder, LtRequest)
    assert RequestWrapper.from_request(remainder).subject.attr_name == 'id'
    assert RequestWrapper.from_request(remainder).other.value == 3
