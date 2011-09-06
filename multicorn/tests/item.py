# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
import attest

from datetime import time, datetime, date
from multicorn.item.base import BaseItem
from multicorn.item.multi import MultiValueItem


suite = Tests()


@suite.test
def test_dict():
    """Test ithe test with a working dict implementation"""
    test_dict_behaviour(dict)

    def make_item(clazz):
        class Dummy:
            pass
        corn = Dummy()
        corn.properties = dict(
            (name, int) for name in ('foo', 'bar', 'baz', 'fuu'))

        def make_it(dict):
            return clazz(corn, dict)
        return make_it

    test_dict_behaviour(dict)
    test_dict_behaviour(make_item(BaseItem))
    test_dict_behaviour(make_item(MultiValueItem))


@suite.test
def test_base_item():
    """Test BaseItem behaviour"""
    test_values(BaseItem)

    def baz_loader(item):
        baz_loader.calls += 1
        return 8

    class Fuu_loader():
        value = None

        def __call__(self, item):
            if self.value is None:
                self.calls += 1
                self.value = 10 + 2
            return self.value

    test_lazy_values(BaseItem, baz_loader, Fuu_loader)


@suite.test
def test_multi_value_item():
    """Test MultiValueItem behaviour"""
    test_values(MultiValueItem)

    def baz_loader(item):
        baz_loader.calls += 1
        return [8, 7, 9]

    class Fuu_loader():
        value = None

        def __call__(self, item):
            if self.value is None:
                self.calls += 1
                self.value = [12, 87, 0]
            return self.value

    test_lazy_values(MultiValueItem, baz_loader, Fuu_loader)

    # Multi dict specific tests
    class Dummy:
        pass
    corn = Dummy()
    corn.properties = dict(
        (name, int) for name in ('foo', 'bar', 'baz', 'fuu'))

    item = MultiValueItem(corn, {'foo': [3, 2],
                                 'bar': (5, "baz", None),
                                 'baz': '__',
                                 'fuu': None})
    assert len(item) == 4
    assert set(item) == set(['foo', 'bar', 'baz', 'fuu'])
    assert set(item.keys()) == set(['foo', 'bar', 'baz', 'fuu'])
    assert set(item.iterkeys()) == set(['foo', 'bar', 'baz', 'fuu'])
    assert set(item.values()) == set([3, 5, '__', None])
    assert set(item.itervalues()) == set([3, 5, '__', None])
    assert set(item.items()) == set([('bar', 5),
                                     ('foo', 3),
                                     ('fuu', None),
                                     ('baz', '__')])
    assert set(item.iteritems()) == set([('bar', 5),
                                     ('foo', 3),
                                     ('fuu', None),
                                     ('baz', '__')])

    for i in xrange(2):
        assert item['foo'] == 3
        assert item['bar'] == 5
        assert item['baz'] == '__'
        assert item['fuu'] == None
    for i in xrange(2):
        assert item.getlist('foo') == (3, 2)
        assert item.getlist('bar') == (5, "baz", None)
        assert item.getlist('baz') == ('__',)
        assert item.getlist('fuu') == (None,)

    for key, values in item.itemslist():
        assert item.getlist(key) == values
    for key, values in item.iteritemslist():
        assert item.getlist(key) == values

    assert set(item.valueslist()) == set(
        [(5, "baz", None), (3, 2), (None,), ('__',)])

    # Assignations
    item['foo'] = "Singleton"
    assert item['foo'] == "Singleton"
    assert item.getlist('foo') == ("Singleton",)
    for list_ in ((1, "2", "three"), [1, "2", "three"], {1, "2", "three"}):
        item.setlist('foo', list_)
        assert item['foo'] == 1
        assert item.getlist('foo') == (1, "2", "three")


def test_dict_behaviour(dict):
    """Test a dict-like behaviour"""
    assert not dict({}) is True
    assert not dict({'foo': 1}) is False
    item = dict({'foo': 3, 'bar': 5,
                'baz': '__', 'fuu': None})
    assert len(item) == 4
    assert set(item) == set(['foo', 'bar', 'baz', 'fuu'])
    assert set(item.keys()) == set(['foo', 'bar', 'baz', 'fuu'])
    assert set(item.iterkeys()) == set(['foo', 'bar', 'baz', 'fuu'])
    assert set(item.values()) == set([3, 5, '__', None])
    assert set(item.itervalues()) == set([3, 5, '__', None])
    assert set(item.items()) == set([('bar', 5),
                                     ('foo', 3),
                                     ('fuu', None),
                                     ('baz', '__')])
    assert set(item.iteritems()) == set([('bar', 5),
                                     ('foo', 3),
                                     ('fuu', None),
                                     ('baz', '__')])
    assert 'bar' in item
    assert 'baz' in item
    assert 'fuu' in item
    assert 'Lipsum' not in item
    for i in xrange(2):
        assert item['foo'] == 3
        assert item['bar'] == 5
        assert item['baz'] == '__'
        assert item['fuu'] == None
    # Assignation
    for i in (True, False, 0, 12, -87293284792374973423948739, "", "srtsr",
              (), (1,), (" ", u"é", 3),
              [], ['a', '_b'], {1, 45, "_"}, {'a': 1, 1: 'a'},
              time(12, 3, 1), date.today(), datetime.now(), None):
        item['bar'] = i
        assert item['bar'] == i
        assert item.get('bar') == i
        item['foo'] = i


def test_values(clazz):
    """Test basic values item behaviour"""
    class Dummy:
        pass
    corn = Dummy()
    corn.properties = dict(
        (name, int) for name in ('foo', 'bar', 'baz', 'fuu'))

    item = clazz(corn, {'foo': 3, 'bar': 5,
                        'baz': '__', 'fuu': None})
    assert len(item) == 4
    assert set(item) == set(['foo', 'bar', 'baz', 'fuu'])
    assert 'bar' in item
    assert 'baz' in item
    assert 'fuu' in item
    assert 'Lipsum' not in item
    for i in xrange(2):
        assert item['foo'] == 3
        assert item['bar'] == 5
        assert item['baz'] == '__'
        assert item['fuu'] == None

    # Reassignation
    item['baz'] = 13
    assert item['baz'] == 13
    item['fuu'] = "-_-"
    assert item['fuu'] == "-_-"
    item['foo'] = (1, 3, 5)
    assert item['foo'] == (1, 3, 5)

    with attest.raises(KeyError):
        item['Lorem'] = 33

    with attest.raises(TypeError):
        del item['foo']
    with attest.raises(TypeError):
        del item['ispum']

    # Raise on extra properties
    with attest.raises(ValueError) as extra:
        clazz(
            corn,
            {'foo': 3, 'bar': 5, 'oof': 1})
    assert extra.args == ("Unexpected properties: ('oof',)",)

    with attest.raises(ValueError) as extra:
        clazz(
            corn,
            {'foo': 3, 'bar': 5, 'oof': 1, 'rab': 1})
    assert "Unexpected properties:" in extra.args[0]


def test_lazy_values(clazz, fun_loader, class_loader):
    """Test lazy values item behaviour"""
    class Dummy:
        pass
    corn = Dummy()
    corn.properties = dict(
        (name, int) for name in ('foo', 'bar', 'baz', 'fuu'))

    fuu_loader = class_loader()
    fuu_loader.calls = 0

    fun_loader.calls = 0
    item = clazz(corn,
                 {'foo': 3, 'bar': 5},
                 {'baz': fun_loader, 'fuu': fuu_loader})
    assert len(item) == 4
    assert set(item) == set(['foo', 'bar', 'baz', 'fuu'])
    assert 'bar' in item
    assert 'baz' in item
    assert 'fuu' in item
    assert 'Lipsum' not in item
    assert item['foo'] == 3
    # The loader was not called yet.
    assert fun_loader.calls == 0

    assert item['baz'] == 8
    # Get a second time.
    assert item['foo'] == 3
    assert item['baz'] == 8
    # The loader was called twice.
    assert fun_loader.calls == 2

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

    fun_loader.calls = 0
    fuu_loader.calls = 0
    item = clazz(corn,
                    {'foo': 3, 'bar': 5},
                    {'baz': fun_loader, 'fuu': fuu_loader})

    item['baz'] = 13
    assert item['baz'] == 13
    item['fuu'] = 29
    assert item['fuu'] == 29
    item['foo'] = 21
    assert item['foo'] == 21

    # The loaders were never called
    assert fun_loader.calls == 0
    assert fuu_loader.calls == 0

    with attest.raises(KeyError):
        item['Lorem'] = 33

    with attest.raises(TypeError):
        del item['foo']
    with attest.raises(TypeError):
        del item['ispum']

    # Raise on extra properties
    with attest.raises(ValueError) as extra:
        clazz(
            corn,
            {'foo': 3, 'bar': 5, 'oof': 1},
            {'baz': fun_loader, 'fuu': fuu_loader})
    assert extra.args == ("Unexpected properties: ('oof',)",)

    with attest.raises(ValueError) as extra:
        clazz(
            corn,
            {'foo': 3, 'bar': 5},
            {'baz': fun_loader, 'fuu': fuu_loader, 'rab': lambda x: x})
    assert extra.args == ("Unexpected properties: ('rab',)",)

    with attest.raises(ValueError) as extra:
        clazz(
            corn,
            {'foo': 3, 'bar': 5, 'oof': 1},
            {'baz': fun_loader, 'fuu': fuu_loader, 'rab': lambda x: x})
    assert "Unexpected properties:" in extra.args[0]
