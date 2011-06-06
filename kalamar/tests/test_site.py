# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Site test.

Test registering access points to a Site.

"""

from nose.tools import eq_, raises

from kalamar.site import Site, _translate_request
from kalamar.request import Condition, And, Or, Not
from kalamar.access_point import AlreadyRegistered, AccessPoint


class DummyAccessPoint(AccessPoint):
    """Dummy access point for testing purpose."""
    properties = identity_properties = site = name = None
    delete = save = search = None

    def __init__(self):
        super(DummyAccessPoint, self).__init__({}, {})


def test_simple_setup():
    """Setup a Site with a single access point, no exception raised."""
    site = Site()
    access_point = DummyAccessPoint()
    site.register("things", access_point)
    eq_(site.access_points, {"things": access_point})
    
@raises(AlreadyRegistered)
def test_double_register():
    """Registering the same AP twice raises an exception."""
    site = Site()
    access_point = DummyAccessPoint()
    site.register("things", access_point)
    site.register("stuff", access_point)
    
@raises(RuntimeError)
def test_ap_name_conflict():
    """Registering two APs with the same name raises an exception."""
    site = Site()
    site.register("things", DummyAccessPoint())
    site.register("things", DummyAccessPoint())

def test_translate_request():
    """Test request translations."""
    aliases = {"id": "id", "name": "name", "title": "name"}
    eq_(_translate_request(Condition("id", "=", 1), aliases),
        Condition("id", "=", 1))
    eq_(_translate_request(Condition("name", "=", "foo"), aliases),
        Condition("name", "=", "foo"))
    eq_(_translate_request(
            Not(Or(Condition("id", "=", 7),
                   Condition("title", "!=", "spam"))), aliases),
        Not(Or(Condition("id", "=", 7),
               Condition("name", "!=", "spam"))))
    eq_(_translate_request(
            And(Condition("id", "=", 4),
                Not(Or(Condition("name", "=", "spam"),
                       Condition("title", "!=", "egg")))), aliases),
        And(Condition("id", "=", 4),
            Not(Or(Condition("name", "=", "spam"),
                   Condition("name", "!=", "egg")))))
