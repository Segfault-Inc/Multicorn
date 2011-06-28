# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.pop import Pop, LazyPop
import types

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
    assert item.id == 1
    assert item.name == "foo"
    assert item.lastname == "bar"
    itemkw = Pop(Corn).one(id=1)
    assert item == itemkw


@suite.test
def test_pop_get(Corn):
    """ Test pop get """
    items = Pop(Corn).get()
    assert len(items) == 3
    items = Pop(Corn).get(c.id > 1)
    assert len(items) == 2
    assert Pop(Corn).get(sort=c.id)[1].id == 2


@suite.test
def test_lazy_pop(Corn):
    """ Test lazy pop """
    lazy_items = LazyPop(Corn).get()
    assert isinstance(lazy_items, types.GeneratorType)
    assert len(list(lazy_items)) == 3
    assert len(list(lazy_items)) == 0

    items = Pop(Corn).get()
    lazy_items = LazyPop(Corn).get()
    assert items == list(lazy_items)


@suite.test
def test_pop_alias(Corn):
    """ Test pop alias """
    items = list(
        Pop(Corn).alias(
            {'nom': c.lastname,
             'prenom': c.name,
             'i': c.id
             }, c.prenom.matches("foo"),
            c.i))
    assert len(items) == 2
    assert items[0] == {
        'i': 1,
        'prenom': u'foo',
        'nom': u'bar'}
    assert items[1] == {
        'i': 3,
        'prenom': u'foo',
        'nom': u'baz'}
