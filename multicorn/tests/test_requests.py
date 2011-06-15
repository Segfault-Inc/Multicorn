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
def test_logical_simplifications():
    for true, false in ((True, False), (1, 0), ('a', '')):
        assert repr(c.foo & true) == "Attribute[Context[0], 'foo']"
        assert repr(c.foo | false) == "Attribute[Context[0], 'foo']"
        assert repr(c.foo & false) == 'False'
        assert repr(c.foo | true) == 'True'

        assert repr(true & c.foo) == "Attribute[Context[0], 'foo']"
        assert repr(false | c.foo) == "Attribute[Context[0], 'foo']"
        assert repr(false & c.foo) == 'False'
        assert repr(true | c.foo) == 'True'

    assert repr(~c.foo) == "Not[Attribute[Context[0], 'foo']]"
    assert repr(~literal('hello')) == 'False'
    assert repr(~literal('')) == 'True'

    # Augmented assignment doesn't need to be defined explicitly
    a = b = c.foo
    assert not hasattr(type(a), '__iadd__')
    a &= c.bar
    assert repr(a) == \
        "And[Attribute[Context[0], 'foo'], Attribute[Context[0], 'bar']]"
    # No in-place mutation
    assert repr(b) == "Attribute[Context[0], 'foo']"


@suite.test
def test_boolean_logic():
    # Not literals so that simplification does not happen.
    true = literal(1) == 1
    false = literal(1) == 0

    print true, ~true
    assert_value(~true, False)
    assert_value(~false, True)

    assert_value(true & true, True)
    assert_value(true & false, False)
    assert_value(false & true, False)
    assert_value(false & false, False)

    assert_value(true | true, True)
    assert_value(true | false, True)
    assert_value(false | true, True)
    assert_value(false | false, False)

    assert_value(true ^ true, False)
    assert_value(true ^ false, True)
    assert_value(false ^ true, True)
    assert_value(false ^ false, False)


@suite.test
def test_comparaisons():
    four = literal(3) + 1
    assert_value(four == 4, True)
    assert_value(four == 3, False)

    assert_value(four != 3, True)
    assert_value(four != 4, False)

    assert_value(four > 3, True)
    assert_value(four > 4, False)
    assert_value(four > 5, False)

    assert_value(four < 3, False)
    assert_value(four < 4, False)
    assert_value(four < 5, True)

    assert_value(four >= 3, True)
    assert_value(four >= 4, True)
    assert_value(four >= 5, False)

    assert_value(four <= 3, False)
    assert_value(four <= 4, True)
    assert_value(four <= 5, True)

    assert_value(4 == four, True)
    assert_value(3 == four, False)

    assert_value(3 != four, True)
    assert_value(4 != four, False)

    assert_value(3 < four, True)
    assert_value(4 < four, False)
    assert_value(5 < four, False)

    assert_value(3 > four, False)
    assert_value(4 > four, False)
    assert_value(5 > four, True)

    assert_value(3 <= four, True)
    assert_value(4 <= four, True)
    assert_value(5 <= four, False)

    assert_value(3 >= four, False)
    assert_value(4 >= four, True)
    assert_value(5 >= four, True)


@suite.test
def test_arithmetic():
    four = literal(3) + 1
    assert_value(four, 4)
    assert_value(-four, -4)
    assert_value(four + 3, 7)
    assert_value(3 + four, 7)
    assert_value(four - 12, -8)
    assert_value(12 - four, 8)
    assert_value(four * 4, 16)
    assert_value(4 * four, 16)
    # TODO: test with and without from __future__ import division
    assert_value(four / 8, .5)
    assert_value(12 / four, 3)


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
def test_indexing_slicing():
    # Repeated for context.
    assert_list(SOURCE.map(c.price), [10, 12, 5])

    assert_value(SOURCE.map(c.price)[0], 10)
    assert_value(SOURCE.map(c.price)[2], 5)
    assert_value(SOURCE.map(c.price)[-3], 10)
    assert_value(SOURCE.map(c.price)[-1], 5)
    with attest.raises(IndexError):
        execute(SOURCE.map(c.price)[9])
    with attest.raises(IndexError):
        execute(SOURCE.map(c.price)[-12])

    assert_list(SOURCE.map(c.price)[1:], [12, 5])
    assert_list(SOURCE.map(c.price)[1:7], [12, 5])
    assert_list(SOURCE.map(c.price)[1:2], [12])

    assert_list(SOURCE.map(c.price)[-2:], [12, 5])
    assert_list(SOURCE.map(c.price)[-2:7], [12, 5])
    assert_list(SOURCE.map(c.price)[-11:], [10, 12, 5])
    assert_list(SOURCE.map(c.price)[-11:7], [10, 12, 5])
    assert_list(SOURCE.map(c.price)[-11:2], [10, 12])
    assert_list(SOURCE.map(c.price)[-2:2], [12])
    assert_list(SOURCE.map(c.price)[1:-1], [12])
    assert_list(SOURCE.map(c.price)[-2:-1], [12])

    assert_list(SOURCE.map(c.price)[::2], [10, 5])
    assert_list(SOURCE.map(c.price)[1::14], [12])
    # TODO: triple check these, I’m not sure what’s the expected behavior.
    assert_list(SOURCE.map(c.price)[::-1], [5, 12, 10])
    assert_list(SOURCE.map(c.price)[1::-1], [5, 12])
    assert_list(SOURCE.map(c.price)[:2:-1], [12, 10])

    # I thought this would be [5, 12] but meh
    assert [10, 12, 5][-2::-1] == [12, 10]
    assert_list(SOURCE.map(c.price)[-2::-1], [12, 10])

    assert [10, 12, 5][:-1:-1] == [] # XXX WTF?
    assert_list(SOURCE.map(c.price)[:-1:-1], [])

    with attest.raises(ValueError):
        execute(SOURCE.map(c.price)[::0])


@suite.test
def test_aggregates():
    assert_list([SOURCE.len()], [3])
    assert_value(SOURCE.len(), 3)
    assert_value(SOURCE.map(c.price).sum(), 27)
    assert_value(SOURCE.map(c.price).min(), 5)
    assert_value(SOURCE.map(c.price).max(), 12)
