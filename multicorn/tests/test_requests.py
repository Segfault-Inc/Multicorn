# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from decimal import Decimal as D
from attest import Tests, assert_hook
import attest

from ..requests import CONTEXT as c
from ..requests.requests import LiteralRequest
from ..python_executor import execute


suite = Tests()


DATA = [
    dict(toto='foo', tata=42, price=10, tax=D('1.196')),
    dict(toto='bar', tata=6, price=12, tax=1),
    dict(toto='bar', tata=42, price=5, tax=D('1.196')),
]

SOURCE = LiteralRequest(DATA)


@suite.test
def test_literal():
    assert execute(SOURCE) is DATA


def r(request, expected):
    assert list(execute(request)) == expected

@suite.test
def test_map():
    r(SOURCE.map(c.price),
      [10, 12, 5])
    r(SOURCE.map(c.price * c.tax),
      [D('11.96'), 12, D('5.98')])
    r(SOURCE.map(c.price * c.tax),
      [D('11.96'), 12, D('5.98')])
    

@suite.test
def test_filter():
    r(SOURCE.filter(),
      DATA)
    r(SOURCE.filter(True),
      DATA)
    r(SOURCE.filter(False),
      [])
    r(SOURCE.filter(c.toto == 'lipsum'),
      [])
    r(SOURCE.filter(c.toto == 'bar').map(c.price),
      [12, 5])
    r(SOURCE.filter(toto='bar').map(c.price),
      [12, 5])
    r(SOURCE.filter(c.price < 11).map(c.price),
      [10, 5])
    r(SOURCE.filter((c.price > 11) | (c.toto == 'foo')).map(c.price),
      [10, 12])
    r(SOURCE.filter((c.price < 11) & (c.toto == 'bar')).map(c.price),
      [5])
    r(SOURCE.filter(c.price < 11, toto='bar').map(c.price),
      [5])


@suite.test
def test_sort():
    r(SOURCE.sort(c.price).map((c.toto, c.tata)),
      [('bar', 42), ('foo', 42), ('bar', 6)])
    r(SOURCE.sort(c.toto).map((c.toto, c.tata)),
      [('bar', 6), ('bar', 42), ('foo', 42)])
    r(SOURCE.sort(c.toto, -c.tata).map((c.toto, c.tata)),
      [('bar', 42), ('bar', 6), ('foo', 42)])
    r(SOURCE.sort(-c.toto, c.tata).map((c.toto, c.tata)),
      [('foo', 42), ('bar', 6), ('bar', 42)])

