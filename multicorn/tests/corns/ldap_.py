# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from multicorn.utils import colorize
from attest import Tests, assert_hook
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.corns.ldap_ import Ldap

# Ldap specific tests
HOSTNAME = "localhost"
PATH = "ou=People,dc=multicorn,dc=org"

suite = Tests()
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


def make_corn():
    mc = Multicorn()

    @mc.register
    @declare(Ldap,
             hostname=HOSTNAME, ldap_path=PATH)
    class Corn(object):
        sn = Property()
        street = Property()
        l = Property()
        telephoneNumber = Property()
        mail = Property()
    return Corn


@suite.test
def test_all():
    Corn = make_corn()
    items = list(Corn.all.execute())
