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
Access point test.

Test various properties of the AccessPoint class.

"""

from kalamar import Site
from kalamar.access_point import AlreadyRegistered
from kalamar.access_point.memory import Memory
from kalamar.property import Property

from nose.tools import eq_, raises, assert_is_instance


def test_auto():
    """Test an access point with auto properties set to ``True``."""
    site = Site()
    access_point = Memory(
        {"id": Property(int, auto=True),
         "name": Property(unicode, auto=True)}, ("id",))
    site.register("things", access_point)

    item = site.create("things", {"id": 1})
    eq_(item["id"], 1)
    name = item["name"]
    assert_is_instance(name, unicode)
    item.save()
    eq_(item["id"], 1)
    eq_(item["name"], name)
    
    item = site.create("things", {"id": 2, "name": "test"})
    eq_(item["id"], 2)
    eq_(item["name"], "test")
    item.save()
    eq_(item["id"], 2)
    eq_(item["name"], "test")
    
    item = site.create("things")
    identifier = item["id"]
    assert_is_instance(identifier, int)
    name = item["name"]
    assert_is_instance(name, unicode)
    item.save()
    eq_(item["id"], identifier)
    eq_(item["name"], name)

def test_auto_not_true():
    """Test an access point with auto properties set to function or tuple."""
    site = Site()
    access_point = Memory(
        {"id": Property(int, auto=lambda: (1000,)),
         "name": Property(unicode, auto=("test",))}, ("id",))
    site.register("things", access_point)

    item = site.create("things", {"id": 1})
    eq_(item["id"], 1)
    eq_(item["name"], "test")
    item.save()
    eq_(item["id"], 1)
    eq_(item["name"], "test")
    
    item = site.create("things", {"id": 2, "name": "test"})
    eq_(item["id"], 2)
    eq_(item["name"], "test")
    item.save()
    eq_(item["id"], 2)
    eq_(item["name"], "test")
    
    item = site.create("things")
    eq_(item["id"], 1000)
    eq_(item["name"], "test")
    item.save()
    eq_(item["id"], 1000)
    eq_(item["name"], "test")

@raises(ValueError)
def test_bad_auto():
    """Test an access point with auto properties set to a bad value."""
    site = Site()
    access_point = Memory(
        {"id": Property(int, auto=(1000,)),
         "name": Property(unicode, auto="test")}, ("id",))
    site.register("things", access_point)

    item = site.create("things", {"id": 1})

@raises(AlreadyRegistered)
def test_already_registered():
    """Try to register an access_point twice."""
    access_point = Memory(
        {"id": Property(int, auto=(1000,)),
         "name": Property(unicode, auto="test")}, ("id",))
    site = Site()
    site.register("things", access_point)
    site = Site()
    site.register("spams", access_point)

def test_non_mandatory():
    """A non mandatory property set to None should return None."""
    access_point = Memory(
        {"id": Property(int), "name": Property(unicode)}, ("id",))
    site = Site()
    site.register("things", access_point)
    item = site.create("things", {"id": 1, "name": None})
    eq_(item.getlist("name"), (None,))

@raises(ValueError)
def test_bad_value():
    """Property of bad value should raise ValueError."""
    access_point = Memory(
        {"id": Property(int), "name": Property(int)}, ("id",))
    site = Site()
    site.register("things", access_point)
    site.create("things", {"id": 1, "name": "toto"})

def test_unknown_type():
    """Check that an unknown type is correctly cast."""
    access_point = Memory(
        {"id": Property(int), "name": Property(complex)}, ("id",))
    site = Site()
    site.register("things", access_point)
    item = site.create("things", {"id": 1, "name": "1+j"})
    eq_(item["name"], 1+1j)

@raises(KeyError)
def test_adding_property():
    """Adding a property to an item fails."""
    access_point = Memory(
        {"id": Property(int), "name": Property(unicode)}, ("id",))
    site = Site()
    site.register("things", access_point)
    item = site.create("things", {"id": 1, "name": "toto"})
    item["eggs"] = "spam"
