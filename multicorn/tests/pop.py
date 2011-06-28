# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.pop import Pop

suite = Tests()


@suite.context
def context():
    mc = Multicorn()
    corn = make_corn()
    mc.register(corn)
    yield corn


def make_corn():
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    return Corn


@suite.test
def test_pop_one(Corn):
    """ Test pop one """
    item = Pop(Corn).one(c.id == 1)
    assert item == {'id': 1, 'name': u'foo', 'lastname': u'bar'}
    itemkw = Pop(Corn).one(id=1)
    assert item == itemkw
    items = list(Pop(Corn).get())
    assert len(items) == 3
    items = list(Pop(Corn).get(c.id > 1))
    assert len(items) == 2
