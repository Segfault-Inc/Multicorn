# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from attest import assert_hook
from multicorn.requests import CONTEXT as c


TESTS = []

def corntest(fun):
    TESTS.append(fun)
    return fun


def make_data(Corn):
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()


@corntest
def emptyness(Corn):
    """ Tests if the corn is clean """
    # Assert we are on an empty corn :
    corn_len = Corn.all.len().execute()
    assert corn_len == 0

    items = list(Corn.all.execute())
    assert len(items) == 0


@corntest
def save(Corn):
    """ Tests the creation of an element """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3


@corntest
def existence(Corn):
    """ Tests the existence of created elements """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3

    item = Corn.all.filter(c.id == 1).one().execute()
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'
    assert item.corn is Corn # This is an actual Item, not a dict.

    item = Corn.all.filter(c.id == 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'
    assert item.corn is Corn

    item = Corn.all.filter(c.id == 3).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'
    assert item.corn is Corn

    same_item = Corn.all.filter(c.id == 3).one().execute()
    assert same_item == item
    # The Corn always return new Item instance
    assert same_item is not item


@corntest
def add(Corn):
    """ Tests adding an element """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    Corn.create({'id': 10, 'name': 'ban', 'lastname': 'bal'}).save()
    items = list(Corn.all.filter(c.id == 10).execute())
    assert len(items) == 1
    item = items[0]
    assert item['id'] == 10
    assert item['name'] == 'ban'
    assert item['lastname'] == 'bal'


@corntest
def delete(Corn):
    """ Tests deleting an element """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    item = Corn.all.filter(c.id == 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'
    item.delete()
    items = list(Corn.all.execute())
    assert len(items) == 2
    assert Corn.all.filter(c.id == 1).len().execute() == 1
    assert Corn.all.filter(c.id == 2).len().execute() == 0
    assert Corn.all.filter(c.id == 3).len().execute() == 1


@corntest
def basic_query(Corn):
    """ Tests basic querying """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.id == c.id).execute())
    assert len(items) == 3

    item = Corn.all.filter(c.id == 2).one().execute()
    item2 = Corn.all.filter(2 == c.id).one().execute()
    assert item == item2

    items = list(Corn.all.filter(
        c.name + "<" + c.lastname + ">" == "foo<bar>").execute())
    assert len(items) == 1
    item = items[0]
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'

    items = list(Corn.all.filter(
        c.name + u"«" + c.lastname + u"»" == u"foo«bar»").execute())
    assert len(items) == 1
    item = items[0]
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'

    item = Corn.all.filter(c.id - 1 == 2).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'

    item = Corn.all.filter(1 == c.id / 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'

    item = Corn.all.filter((c.id * c.id) == 9).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'

    item = Corn.all.filter(c.id ** c.id == c.id).one().execute()
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'


@corntest
def maps(Corn):
    """Test various map operations"""
    make_data(Corn)
    items = list(Corn.all.map(c.id).execute())
    assert len(items) == 3
    test = all([x in items for x in [1, 2, 3]])
    assert test
    items = list(Corn.all.map(c.id * c.id).execute())
    assert len(items) == 3
    test = all([x in items for x in [1, 4, 9]])
    assert test
    items = list(Corn.all.map(c.name + ' ' + c.lastname).execute())
    assert len(items) == 3
    test = all([x in items for x in ['foo bar', 'baz bar', 'foo baz']])
    assert test
    item = Corn.all.map(
        c.name + ' ' + c.lastname).filter(c == 'foo bar').one().execute()
    assert item == 'foo bar'
    item = Corn.all.map(
        c.name + u'ùþß' + c.lastname).filter(c == u'fooùþßbar').one().execute()
    assert item == u'fooùþßbar'
    items = list(Corn.all.map(
        {'foo': c.name}).filter(c.foo == 'baz').execute())
    assert len(items) == 1
    assert all(type(item) == dict for item in items)
    assert all(
        item['foo'] == 'baz' for item in items)
    fullname = c.name + ' ' + c.lastname
    item = Corn.all.map(fullname).filter(c == 'foo bar').one().execute()
    assert item == 'foo bar'
    item = Corn.all.map({'fullname': fullname, 'id': c.id}).filter(
            (c.id * c.id) == 1).one().execute()
    assert item == {'fullname': 'foo bar', 'id': 1}

@corntest
def aggregates(Corn):
    """Test aggregates"""
    make_data(Corn)
    max = Corn.all.max(c.id).execute()
    assert max == 3
    min = Corn.all.min(c.id).execute()
    assert min == 1
    len = Corn.all.len().execute()
    assert len == 3

@corntest
def filter(Corn):
    """Test various filters"""
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.name == 'foo' ).execute())
    assert len(items) == 2
    assert all([item['name'] == 'foo' for item in items])
    items = list(Corn.all.filter((c.name == 'foo' ) &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    items = list(Corn.all.filter((c.name == 'baz' ) |
        (c.lastname == 'baz')).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    items = list(Corn.all.filter(c.name == 'foo').execute())
    assert len(items) == 2
    assert all([item['name'] == 'foo' for item in items])
    items = list(Corn.all.filter((c.name == 'foo' ) &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter((c.name == 'baz' ) |
        (c.lastname == 'baz')).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id < 2).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter((c.id < 3) & (c.id > 1)).execute())
    assert len(items) == 1
    assert items[0]['id'] == 2
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id >= 2).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id <= 2).execute())
    assert len(items) == 2
    assert 1 in (x['id'] for x in items)
    assert 2 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id != 2).execute())
    assert len(items) == 2
    assert 1 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all([[item.corn == Corn for item in items]])

@corntest
def test_groupby(Corn):
    """Test groupby requests"""
    make_data(Corn)
    items = list(Corn.all.groupby(c.name, {
        'max': c.max(c.id),
        'min': c.min(c.id),
        'sum': c.sum(c.id)}).sort(c.key).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz' and items[0]['group'] == {'max': 2, 'min': 2, 'sum': 2}
    assert items[1]['key'] == 'foo' and items[1]['group'] == {'max': 3, 'min': 1, 'sum': 4}
    items = list(Corn.all.groupby(c.name, c.sum(c.id)).sort(c.group).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz' and items[0]['group'] == 2
    assert items[1]['key'] == 'foo' and items[1]['group'] == 4
    items = list(Corn.all.groupby(c.name, c.len()).sort(c.group).execute())
    assert len(items) == 2
    assert items[0]['group'] == 1
    assert items[1]['group'] == 2
    items = list(Corn.all.groupby(c.name, c.len()).sort(c.group).map(
        c.key + ' : ' + c.group.str() + ' items').execute())
    assert len(items) == 2
    assert items[0] == 'baz : 1 items'
    assert items[1] == 'foo : 2 items'
