# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
import attest

from ..item import BaseItem
from ..property import BaseProperty


suite = Tests()


@suite.test
def test_item():
    class Dummy:
        pass
    corn = Dummy()
    corn.properties = [BaseProperty(name, int)
                       for name in ('foo', 'bar', 'baz')]
    
    def baz_loader():
        baz_loader.calls += 1
        return 8

    baz_loader.calls = 0
    item = BaseItem(corn, {'foo': 3, 'bar': 5}, {'baz': baz_loader})
    assert len(item) == 3
    assert set(item) == set(['foo', 'bar', 'baz'])
    assert 'bar' in item
    assert 'baz' in item
    assert 'Lipsum' not in item
    assert item['foo'] == 3
    # The loader was not called yet.
    assert baz_loader.calls == 0
    assert item['baz'] == 8
    # Get a second time.
    assert item['foo'] == 3
    assert item['baz'] == 8
    # The loader was called only once.
    assert baz_loader.calls == 1

    baz_loader.calls = 0
    item = BaseItem(corn, {'foo': 3, 'bar': 5}, {'baz': baz_loader})
    item['baz'] = 13
    assert item['baz'] == 13
    item['foo'] = 21
    assert item['foo'] == 21
    # The loader was never called
    assert baz_loader.calls == 0

    with attest.raises(KeyError):
        item['Lorem'] = 33

    with attest.raises(TypeError):
        del item['foo']
    with attest.raises(TypeError):
        del item['ispum']

