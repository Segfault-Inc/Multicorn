# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Normalize test.

Test the alchemy backend on an sqlite base

"""

from nose.tools import eq_

from kalamar.property import Property
from kalamar.request import normalize, Condition, And, Or, Not


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


def test_normalize():
    """Assert the normalize function works properly."""
    # TODO: more unit tests here.
    
    properties = {"a": Property(int), "b": Property(unicode)}
    eq_(normalize(properties, {u"a": 1, u"b": u"foo"}),
        And(Condition(u"a", "=", 1), Condition(u"b", "=", u"foo")))
    properties = {"a": Property(float), "b": Property(unicode)}
    eq_(normalize(properties, {u"a": 1, u"b": "foo"}),
        And(Condition(u"a", "=", 1.0), Condition(u"b", "=", u"foo")))

    properties = {"a": Property(float), "b": Property(int)}
    try:
        normalize(properties, {u"a": 1, u"b": "foo"})
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError."

