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
def test_attributes():
    assert repr(c.foo) == "c.foo"
    with attest.raises(AttributeError):
        c.foo = 4
    assert_value(literal({'foo': 12}).foo, 12)
    with attest.raises(KeyError):
        execute(literal({'foo': 12}).bar)


@suite.test
def test_logical_simplifications():
    for true, false in ((True, False), (1, 0), ('a', '')):
        assert repr(c.foo & true) == "c.foo"
        assert repr(c.foo | false) == "c.foo"
        assert repr(c.foo & false) == 'literal(False)'
        assert repr(c.foo | true) == 'literal(True)'

        assert repr(true & c.foo) == "c.foo"
        assert repr(false | c.foo) == "c.foo"
        assert repr(false & c.foo) == 'literal(False)'
        assert repr(true | c.foo) == 'literal(True)'

    assert repr(~c.foo) == "~c.foo"
    assert repr(~literal('hello')) == 'literal(False)'
    assert repr(~literal('')) == 'literal(True)'

    # Augmented assignment doesn't need to be defined explicitly
    a = b = c.foo
    assert not hasattr(type(a), '__iadd__')
    a &= c.bar
    assert repr(a) == "(c.foo & c.bar)"
    # No in-place mutation
    assert repr(b) == "c.foo"


@suite.test
def test_boolean_logic():
    # Not literals so that simplification does not happen.
    true = literal(1) == 1
    false = literal(1) == 0

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
def test_fancy_add():
    assert_list(literal([1, 3]) + [42, 9], [1, 3, 42, 9])
    assert_value(
        literal({'foo': 3, 'bar': 5}) + {'bar': 8, 'buzz': 13},
        {'foo': 3, 'bar': 8, 'buzz': 13}
    )
    assert_list(SOURCE.map(c.price) + ['lipsum'], [10, 12, 5, 'lipsum'])


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
    assert_list(
            SOURCE.map(c.toto.upper()),
            ['FOO', 'BAR', 'BAR']
    )
    assert_list(
            SOURCE.map(c.toto.upper() + c.toto.lower()),
            ['FOOfoo', 'BARbar', 'BARbar']
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
            SOURCE.filter(c.toto.matches('^b.r$')).map(c.price),
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

    with attest.raises(TypeError):
        execute(SOURCE.map(c.price)[1:, 4])

    with attest.raises(TypeError):
        execute(SOURCE.map(c.price)['a'])


@suite.test
def test_aggregates():
    assert_list([SOURCE.len()], [3])
    assert_value(SOURCE.len(), 3)
    assert_value(SOURCE.filter(c.toto == 'bar').len(), 2)
    assert_value(SOURCE.map(c.price).sum(), 27)
    assert_value(SOURCE.map(c.price).min(), 5)
    assert_value(SOURCE.map(c.price).max(), 12)


@suite.test
def test_distinct():
    assert_list(SOURCE.sort(c.price).map(c.toto), ['bar', 'foo', 'bar'])
    assert_list(SOURCE.sort(c.price).map(c.toto).distinct(), ['bar', 'foo'])

    # Non-hashable values: XXX not supported yet.
    assert_list(
        literal([
            {'foo': 4, 'bar': 5},
            {'foo': 4, 'bar': 143},
            {'foo': 4, 'bar': 5},
        ]).distinct(),
        [
            {'foo': 4, 'bar': 5},
            {'foo': 4, 'bar': 143},
        ]
    )


@suite.test
def test_one():
    assert_value(SOURCE.filter(c.toto == 'foo').map(c.price).one(), 10)

    with attest.raises(IndexError) as error:
        execute(SOURCE.filter(c.toto == 'fizzbuzz').one())
    assert error.args == ('.one() on an empty sequence',)

    with attest.raises(ValueError) as error:
        execute(SOURCE.filter(c.toto == 'bar').one())
    assert error.args == ('More than one element in .one()',)


@suite.test
def test_groupby():
    assert_list(
        SOURCE.groupby(c.tata, elements=c).sort(-c.key),
        [
            {'key': 42, 'elements': [
                dict(toto='foo', tata=42, price=10, tax=D('1.196')),
                dict(toto='bar', tata=42, price=5, tax=D('1.196')),
             ]},
            {'key': 6, 'elements': [
                dict(toto='bar', tata=6, price=12, tax=1),
             ]},
        ]
    )
    assert_list(
        SOURCE.groupby(c.tata, len=c.len()).sort(-c.key),
        [
            {'key': 42, 'len': 2},
            {'key': 6, 'len': 1},
        ]
    )
    result = list(execute(
        SOURCE.groupby(c.tata, group=c).sort(-c.key).map(
            c + {'group': c.group.map(-c.price * c.tax + c(-1).key)}
        )
    ))
    for line in result:
        line['group'] = list(line['group'])
    assert result == [
        {'key': 42, 'group': [D('30.04'), D('36.02')]},
        {'key': 6, 'group': [-6]},
    ]
    assert_list(
        SOURCE.groupby(c.tata, group=c).sort(-c.key).map(
            (c.key, c.group.map(c.price).sum() - c.key)
        ),
        [
            (42, -27),
            (6, 6),
        ]
    )

    # group by non-hashables
    assert_list(
        literal([
            {'foo': 4, 'bar': 5},
            {'foo': 4, 'bar': 143},
            {'foo': 4, 'bar': 5},
        ]).groupby(c, group=c).map((c.key, c.group.len())),
        [
            ({'foo': 4, 'bar': 5}, 2),
            ({'foo': 4, 'bar': 143}, 1),
        ]
    )


@suite.test
def test_case():
    from ..requests import case, when
    from ..requests import ARGUMENT_NOT_GIVEN

    assert (repr(case(when(c.age > 18, c.pics), c.text)) ==
            "Case[(When[(c.age > literal(18)), c.pics],), c.text]")
    assert repr(case(when(c.age > 18, c.pics), when(c.age <= 18, c.text)) ==
                "Case[(When[(c.age > literal(18)), c.pics],"
                "When[(c.age <= literal(18)), c.text]),"
                "%r]" % ARGUMENT_NOT_GIVEN)
