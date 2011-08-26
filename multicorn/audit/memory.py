# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import assert_hook

from multicorn.corns.memory import Memory
from multicorn.declarative import declare, Property


from . import make_test_suite


def make_corn():
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn

# Generic tests
suite = make_test_suite(make_corn, 'memory')
