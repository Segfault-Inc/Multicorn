# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from decimal import Decimal as D
from attest import Tests, assert_hook
import attest

from ..requests import CONTEXT as c, literal
from ..python_executor import execute


suite = Tests()


DATA = [
    dict(toto='foo', tata=42, price=10, tax=D('1.196')),
    dict(toto='bar', tata=6, price=12, tax=1),
    dict(toto='bar', tata=42, price=5, tax=D('1.196')),
]

SOURCE = literal(DATA)


@suite.test
def test_literal():
    assert execute(SOURCE) is DATA


def assert_list(request, expected):
    assert list(execute(request)) == expected


def assert_value(request, expected):
    assert execute(request) == expected


@suite.test
def test_map():
    assert_list(
        SOURCE.map(c.price),
        [10, 12, 5]
    )
    assert_list(
        SOURCE.map(c.price * c.tax),
        [D('11.96'), 12, D('5.98')]
    )
    assert_list(
        SOURCE.map(c.price * c.tax),
        [D('11.96'), 12, D('5.98')]
    )


@suite.test
def test_filter():
    assert_list(
        SOURCE.filter(),
        DATA
    )
    assert_list(
        SOURCE.filter(True),
        DATA
    )
    assert_list(
        SOURCE.filter(False),
        []
    )
    assert_list(
        SOURCE.filter(c.toto == 'lipsum'),
        []
    )
    assert_list(
        SOURCE.filter(c.toto == 'bar').map(c.price),
        [12, 5]
    )
    assert_list(
        SOURCE.filter(toto='bar').map(c.price),
        [12, 5]
    )
    assert_list(
        SOURCE.filter(c.price < 11).map(c.price),
        [10, 5]
    )
    assert_list(
        SOURCE.filter((c.price > 11) | (c.toto == 'foo')).map(c.price),
        [10, 12]
    )
    assert_list(
        SOURCE.filter((c.price < 11) & (c.toto == 'bar')).map(c.price),
        [5]
    )
    assert_list(
        SOURCE.filter(c.price < 11, toto='bar').map(c.price),
        [5]
    )


@suite.test
def test_sort():
    assert_list(
        SOURCE.sort(c.price).map((c.toto, c.tata)),
        [('bar', 42), ('foo', 42), ('bar', 6)]
    )
    assert_list(
        SOURCE.sort(c.toto).map((c.toto, c.tata)),
        [('bar', 6), ('bar', 42), ('foo', 42)]
    )
    assert_list(
        SOURCE.sort(c.toto, -c.tata).map((c.toto, c.tata)),
        [('bar', 42), ('bar', 6), ('foo', 42)]
    )
    assert_list(
        SOURCE.sort(-c.toto, c.tata).map((c.toto, c.tata)),
        [('foo', 42), ('bar', 6), ('bar', 42)]
    )

    assert_list(
        SOURCE.map(c.price).sort(),
        [5, 10, 12]
    )
    assert_list(
        SOURCE.map(c.price).sort(c),
        [5, 10, 12]
    )
    assert_list(
        SOURCE.map(c.price).sort(-c),
        [12, 10, 5]
    )


@suite.test
def test_aggregates():
    assert_list(
        [SOURCE.len()],
        [3]
    )
    assert_value(
        SOURCE.len(),
        3
    )
    assert_value(
        SOURCE.map(c.price).sum(),
        27
    )
    assert_value(
        SOURCE.map(c.price).min(),
        5
    )
    assert_value(
        SOURCE.map(c.price).max(),
        12
    )
