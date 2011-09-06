# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from multicorn.utils import colorize
from attest import Tests, assert_hook
from multicorn import Multicorn
from multicorn.declarative import declare, Property, computed
from multicorn.requests import CONTEXT as c
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
    class Corn(ComputedExtenser):

        @computed()
        def id(self):
            return c.cn

        @computed()
        def name(self):
            return c.sn

        @computed()
        def lastname(self):
            return c.street

        @id.reverse
        def id(self):
            return {'cn': lambda item: [unicode(item['id'])]}

        @name.reverse
        def name(self):
            return {'sn': lambda item: item['name']}

        @lastname.reverse
        def lastname(self):
            return {'street': lambda item: item['lastname']}

        cn = Property()
        sn = Property()
        street = Property()

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
    suite = make_test_suite(make_corn, 'ldap', teardown)


# @suite.test
# def test_all():
#     Corn = make_corn()
#     items = list(Corn.all.execute())
