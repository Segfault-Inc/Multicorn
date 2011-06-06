# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Access point test.

Test various properties of the AccessPoint class.

"""

import io
# Nose redefines assert_is_instance
# pylint: disable=E0611
from nose.tools import eq_, raises
# pylint: enable=E0611

from kalamar.access_point import AccessPoint, AlreadyRegistered
from kalamar.access_point.memory import Memory
from kalamar.item import Item
from kalamar.property import Property
from kalamar.site import Site
from kalamar.value import PROPERTY_TYPES



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
    assert isinstance(name, unicode)
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
    assert isinstance(identifier, int)
    name = item["name"]
    assert isinstance(name, unicode)
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
    site.create("things", {"id": 1})

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

def test_auto_values():
    """Test that automatic values have correct type."""
    for property_type in PROPERTY_TYPES:
        if property_type not in (io.IOBase, Item):
            auto_value = AccessPoint._auto_value(Property(property_type))
            if property_type is iter:
                assert hasattr(auto_value, "__iter__")
            else:
                assert isinstance(auto_value[0], property_type)
