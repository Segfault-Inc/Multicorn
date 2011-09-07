# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from multicorn.utils import colorize
from attest import Tests, assert_hook
from multicorn import Multicorn
from multicorn.declarative import declare, Property, computed
from multicorn.requests import CONTEXT as c, case, when
from multicorn.corns.extensers.computed import ComputedExtenser
from multicorn.corns.ldap_ import Ldap
from . import make_test_suite

# Ldap specific tests
HOSTNAME = "localhost"
PATH = "ou=People,dc=multicorn,dc=org"
USER = "cn=Manager,dc=multicorn,dc=org"
PASSWORD = "secret"


def make_corn():
    @declare(Ldap, hostname=HOSTNAME, path=PATH, user=USER, password=PASSWORD)
    class Corn(object):
        cn = Property()
        sn = Property()
        l = Property(type=int)

    return Corn


def teardown(corn):
    #Deleting all objects the hardcore way
    for item in corn.all():
        item.delete()

try:
    import ldap
except ImportError:
    import sys
    print(colorize(
        'yellow',
        "WARNING: The LDAP AP is not tested."), file=sys.stderr)
    suite = Tests()
    suite.test = lambda x: None
else:
    suite = Tests()
    # suite = make_test_suite(make_corn, 'ldap', teardown)


@suite.test
def test_ldap():
    mc = Multicorn()
    Corn = make_corn()
    mc.register(Corn)

    assert Corn.all.len()() == 0

    item = Corn.create({'cn': 'foo', 'sn': 'bar', 'l': 5})
    item.save()

    items = list(Corn.all())
    assert len(items) == 1
    item = items[0]
    assert set(item.items()) == set(
        [('cn', 'foo'), ('sn', 'bar'), ('l', 5)])

    item.delete()
    assert Corn.all.len()() == 0
