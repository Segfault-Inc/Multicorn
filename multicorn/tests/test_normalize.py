# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Normalize test.

Test the alchemy backend on an sqlite base

"""

from nose.tools import eq_, raises

from multicorn.property import Property
from multicorn.request import normalize, Condition, And, Or, Not



def test_simplify():
    """Assert that the simplify method reduces the condition tree."""
    condition1 = Condition("a", "=", 1)
    condition2 = Condition("b", ">", 4)

    eq_(And(), And().simplify())
    eq_(And(), And(And()).simplify())
    eq_(And(), And(And(), And()).simplify())
    eq_(Or(), Or().simplify())
    eq_(Or(), Or(Or()).simplify())
    eq_(Or(), Or(Or(), Or()).simplify())

    eq_(condition1, And(condition1).simplify())
    eq_(condition1, And(condition1, condition1).simplify())
    eq_(condition1, And(condition1, And(condition1)).simplify())
    eq_(condition1, And(And(condition1)).simplify())
    eq_(condition1, And(And(condition1), And()).simplify())
    eq_(condition1, Or(condition1).simplify())
    eq_(condition1, Or(condition1, condition1).simplify())
    eq_(condition1, Or(condition1, Or(condition1)).simplify())
    eq_(condition1, Or(Or(condition1)).simplify())
    eq_(condition1, Or(Or(condition1), Or()).simplify())

    eq_(condition1, Not(Not(condition1)).simplify())
    eq_(condition1, Or(Or(), Or(And(condition1))).simplify())
    eq_(And(condition1, condition2),
        And(Or(Or(), Or(And(condition1))), condition2).simplify())


@raises(ValueError)
def test_normalize():
    """Assert the normalize function works properly."""
    # TODO: more unit tests here.
    
    properties = {"a": Property(int), "b": Property(unicode)}
    eq_(normalize(properties, {"a": 1, "b": "foo"}),
        And(Condition("a", "=", 1), Condition("b", "=", "foo")))
    properties = {"a": Property(float), "b": Property(unicode)}
    eq_(normalize(properties, {"a": 1, "b": "foo"}),
        And(Condition("a", "=", 1.0), Condition("b", "=", "foo")))

    properties = {"a": Property(float), "b": Property(int)}
    normalize(properties, {"a": 1, "b": "foo"})

