# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
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
Test the alchemy backend on an sqlite base
"""

from nose.tools import eq_
from kalamar.property import Property
from kalamar.request import _flatten, normalize, Condition, And, Or


def test_flatten():
    eq_(_flatten(And()),
        And())
    eq_(_flatten(And(And(), And())),
        And())
    eq_(_flatten(And(Condition('a', '=', 1))),
        Condition('a', '=', 1))
    eq_(_flatten(Or(Or(), Or(And(Condition('a', '=', 1))))),
        Condition('a', '=', 1))
        
def test_normalize():
    # TODO: more unit tests here.
    
    properties = {'a': Property(int), 'b': Property(unicode)}
    eq_(normalize(properties, {u'a': 1, u'b': u'foo'}),
        And(Condition(u'a', '=', 1), Condition(u'b', '=', u'foo')))
    
    properties = {'a': Property(float), 'b': Property(unicode)}
    eq_(normalize(properties, {u'a': 1, u'b': 'foo'}),
        And(Condition(u'a', '=', 1.0), Condition(u'b', '=', u'foo')))

    properties = {'a': Property(float), 'b': Property(int)}
    try:
        normalize(properties, {u'a': 1, u'b': 'foo'})
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError."

