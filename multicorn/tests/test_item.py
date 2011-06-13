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
                       for name in (
                           'foo', 'bar', 'baz', 'fuu')]

    def baz_loader():
        baz_loader.calls += 1
        return 8

    class Fuu_loader():
        value = None

        def __call__(self):
            if self.value is None:
                self.calls += 1
                self.value = 10 + 2
            return self.value

    fuu_loader = Fuu_loader()
    fuu_loader.calls = 0

    baz_loader.calls = 0
    item = BaseItem(corn,
                    {'foo': 3, 'bar': 5},
                    {'baz': baz_loader, 'fuu': fuu_loader})
    assert len(item) == 4
    assert set(item) == set(['foo', 'bar', 'baz', 'fuu'])
    assert 'bar' in item
    assert 'baz' in item
    assert 'fuu' in item
    assert 'Lipsum' not in item
    assert item['foo'] == 3
    # The loader was not called yet.
    assert baz_loader.calls == 0
    # With assert_hooks, this is evaluated twice
    # Storing in a variable
    baz = item['baz']
    assert baz == 8
    # Get a second time.
    assert item['foo'] == 3
    baz = item['baz']
    assert baz == 8
    # The loader was called twice.
    assert baz_loader.calls == 2

    # The loader was not called yet.
    assert fuu_loader.calls == 0
    assert item['fuu'] == 12
    assert item['baz'] == 8
    # Get a second time.
    assert item['fuu'] == 12
    assert item['foo'] == 3
    assert item['fuu'] == 12

    # The loader was called once.
    assert fuu_loader.calls == 1

    baz_loader.calls = 0
    fuu_loader.calls = 0
    item = BaseItem(corn,
                    {'foo': 3, 'bar': 5},
                    {'baz': baz_loader, 'fuu': fuu_loader})

    item['baz'] = 13
    assert item['baz'] == 13
    item['fuu'] = 29
    assert item['fuu'] == 29
    item['foo'] = 21
    assert item['foo'] == 21

    # The loaders were never called
    assert baz_loader.calls == 0
    assert fuu_loader.calls == 0

    with attest.raises(KeyError):
        item['Lorem'] = 33

    with attest.raises(TypeError):
        del item['foo']
    with attest.raises(TypeError):
        del item['ispum']

    # Raise on extra properties
    with attest.raises(ValueError) as extra:
        BaseItem(
            corn,
            {'foo': 3, 'bar': 5, 'oof': 1},
            {'baz': baz_loader, 'fuu': fuu_loader})
    assert extra.message == "Unexpected properties: ('oof',)"

    with attest.raises(ValueError) as extra:
        BaseItem(
            corn,
            {'foo': 3, 'bar': 5},
            {'baz': baz_loader, 'fuu': fuu_loader, 'rab': lambda x: x})
    assert extra.message == "Unexpected properties: ('rab',)"

    with attest.raises(ValueError) as extra:
        BaseItem(
            corn,
            {'foo': 3, 'bar': 5, 'oof': 1},
            {'baz': baz_loader, 'fuu': fuu_loader, 'rab': lambda x: x})
    assert "Unexpected properties:" in extra.message

    # Raise on missing properties
    with attest.raises(ValueError) as extra:
        BaseItem(
            corn,
            {'foo': 3},
            {'baz': baz_loader, 'fuu': fuu_loader})
    assert extra.message == "Missing properties: ('bar',)"
    with attest.raises(ValueError) as extra:
        BaseItem(
            corn,
            {'foo': 3, 'bar': 5},
            {'baz': baz_loader})
    assert extra.message == "Missing properties: ('fuu',)"
    with attest.raises(ValueError) as extra:
        BaseItem(
            corn,
            {'foo': 3},
            {'baz': baz_loader})
    assert "Missing properties:" in extra.message
