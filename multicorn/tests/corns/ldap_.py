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
        l = Property()

    return Corn


try:
    import ldap
except ImportError:
    import sys
    print(colorize(
        'yellow',
        "WARNING: The LDAP AP is not tested."), file=sys.stderr)
    emptysuite = Tests()
    emptysuite.test = lambda x: None
    fullsuite = Tests()
    fullsuite.test = lambda x: None
else:
    def teardown(corn):
        # Deleting all objects the hardcore way
        all = corn.ldap.search_s(
            PATH, ldap.SCOPE_ONELEVEL, "objectClass=*", ["dn"])

        for item in all:
            from logging import getLogger
            getLogger("multicorn.ldap").warning("Deleting %s" % item[0])
            corn.ldap.delete_s(item[0])

    data = lambda: tuple([{'cn': '1', 'sn': '4', 'l': 'bar'},
                          {'cn': '2', 'sn': '2', 'l': 'bar'},
                          {'cn': '3', 'sn': '0', 'l': 'baz'}])
    emptysuite, fullsuite = make_test_suite(make_corn, 'ldap', data=data,
                                            teardown=teardown)


@emptysuite.test
def test_optimization_ldap():
    mc = Multicorn()
    Corn = make_corn()
    mc.register(Corn)

    assert Corn.all.len() == 0

    item = Corn.create({'cn': ('foo',), 'sn': ('bar',), 'l': ('5', '_')})
    item.save()

    items = list(Corn.all())
    assert len(items) == 1
    item = items[0]
    assert set(item.itemslist()) == set(
        [('cn', ('foo',)), ('sn', ('bar',)), ('l', ('5', '_'))])

    assert set(item.items()) == set(
        [('cn', 'foo'), ('sn', 'bar'), ('l', '5')])

    item.delete()
    assert Corn.all.len()() == 0
    Corn.create({'cn': ('foo',), 'sn': ('bar',), 'l': ('10', '_')}).save()
    Corn.create({'cn': 'bar', 'sn': 'bat', 'l': ('5', '_', "!")}).save()
    Corn.create({'cn': ('baz',), 'sn': 'bat', 'l': ('6',)}).save()
    assert Corn.all.len()() == 3

    class NotOptimizedError(Exception):
        pass

    def error():
        raise NotOptimizedError

    Corn._all = error
    items = list(Corn.all.filter(c.cn == 'foo')())
    assert len(items) == 1

    items = Corn.all.filter(c.cn != 'bar')()
    assert len(list(items)) == 2

    items = Corn.all.filter(c.l == '5')()
    assert len(list(items)) == 1

    items = Corn.all.filter(c.l >= '1')()
    assert len(list(items)) == 3

    items = Corn.all.filter(c.l <= "7")()
    assert len(list(items)) == 2

    items = Corn.all.filter(c.l <= "6")()
    assert len(list(items)) == 2

    items = Corn.all.filter(c.l < "6")()
    assert len(list(items)) == 1

    items = Corn.all.filter(c.sn != 'bar')()
    assert len(list(items)) == 2
