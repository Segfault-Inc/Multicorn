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
Alchemy test
============

Test the request module.

"""

from nose.tools import assert_equal, assert_not_equal

from kalamar.request import Condition, Not, And, Or


def test_hash_condition():
    """Asserts that the hash method works on Condition"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "tortue")
    condition3 = Condition("name", "<", "hibou")
    condition4 = Condition("name", ">", "tortue")
    condition5 = Condition("bob", "<", "tortue")

    assert_equal(hash(condition1), hash(condition2))
    assert_not_equal(hash(condition1), hash(condition3))
    assert_not_equal(hash(condition1), hash(condition4))
    assert_not_equal(hash(condition1), hash(condition5))

def test_hash_and():
    """Asserts that the hash method works on And"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "hibou")
    condition3 = Condition("name", "<", "tortue")
    
    condition4 = And(condition1, condition2)
    condition5 = And(condition3, condition2)
    condition6 = And(condition1, condition3)

    assert_equal(hash(condition4), hash(condition5))
    assert_not_equal(hash(condition4), hash(condition6))

def test_hash_or():
    """Asserts that the hash method works on Or"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "hibou")
    condition3 = Condition("name", "<", "tortue")
    
    condition4 = Or(condition1, condition2)
    condition5 = Or(condition3, condition2)
    condition6 = Or(condition1, condition3)

    assert_equal(hash(condition4), hash(condition5))
    assert_not_equal(hash(condition4), hash(condition6))

def test_hash_not():
    """Asserts that the hash method works on not"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "hibou")
    condition3 = Condition("name", "<", "tortue")
    
    condition4 = Not(condition1)
    condition5 = Not(condition3)
    condition6 = Not(condition2)

    assert_equal(hash(condition4), hash(condition5))
    assert_not_equal(hash(condition4), hash(condition6))

def test_eq_condition():
    """Assert that the eq operator works on Condition"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "tortue")
    condition3 = Condition("name", "<", "hibou")
    condition4 = Condition("name", ">", "tortue")
    condition5 = Condition("bob", "<", "tortue")

    assert_equal(condition1, condition2)
    assert_not_equal(condition1, condition3)
    assert_not_equal(condition1, condition4)
    assert_not_equal(condition1, condition5)

def test_and():
    """Assert that the eq operator works on And"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "hibou")
    condition3 = Condition("name", "<", "tortue")
    
    condition4 = And(condition1, condition2)
    condition5 = And(condition3, condition2)
    condition6 = And(condition1, condition3)

    assert_equal(condition4, condition5)
    assert_not_equal(condition4, condition6)

def test_or():
    """Assert that the eq operator works on Or"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "hibou")
    condition3 = Condition("name", "<", "tortue")
    
    condition4 = Or(condition1, condition2)
    condition5 = Or(condition3, condition2)
    condition6 = Or(condition1, condition3)

    assert_equal(condition4, condition5)
    assert_not_equal(condition4, condition6)

def test_not():
    """Assert that the eq operator works on Not"""
    condition1 = Condition("name", "<", "tortue")
    condition2 = Condition("name", "<", "hibou")
    condition3 = Condition("name", "<", "tortue")
    
    condition4 = Not(condition1)
    condition5 = Not(condition3)
    condition6 = Not(condition2)

    assert_equal(condition4, condition5)
    assert_not_equal(condition4, condition6)
