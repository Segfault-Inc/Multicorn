# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import assert_hook

from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c

from . import make_test_suite


def make_generic_corn():
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn

# Generic tests
emptysuite, fullsuite = make_test_suite(make_generic_corn, 'memory')


@fullsuite.test
def test_all(Corn, data):
    items = list(Corn.all.execute())
    assert len(items) == 3
    item = (Corn.all.filter(c.id == 1).one().execute())
    assert item['id'] == 1
    assert item['name'] == u'foo'
    assert item['lastname'] == u'bar'


@fullsuite.test
def test_optimization(Corn, data):
    item = Corn.all.map(
        {'id': c.id, 'newname': c.name}).filter(c.id == 1).one().execute()
    assert item['id'] == 1
    assert item['newname'] == u'foo'

    class NotOptimizedError(Exception):
        pass

    def new_all():
        raise NotOptimizedError
    Corn._all = new_all
    item = (Corn.all.filter(c.id == 1).one().execute())
    assert item['id'] == 1
    assert item['name'] == u'foo'
    assert item['lastname'] == u'bar'
    try:
        item = Corn.all.filter((c.id == 1) | (c.name == 'foo')).one().execute()
        assert False, "An exception should have been raised"
    except NotOptimizedError:
        pass
    item = list(Corn.all.filter((c.id == 1) & (c.name == 'baz')).execute())
    assert len(item) == 0
