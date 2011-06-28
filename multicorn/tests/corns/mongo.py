# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests, assert_hook
from multicorn.corns.mongo import Mongo
from multicorn.declarative import declare, Property
from . import make_test_suite
from multicorn.requests import CONTEXT as c


def make_corn():
    @declare(Mongo, identity_properties=("id",),
                 hostname="localhost", port=27017,
                 database="dbtst", collection="mctest")
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


def teardown(corn):
    #Deleting all objects the hardcore way
    corn.db.drop_collection(corn.collection)

try:
    import pymongo
except ImportError:
    import sys
    print >>sys.stderr, "WARNING: The Mongo DB AP is not tested."
    suite = Tests()
else:
    suite = make_test_suite(make_corn, 'mongo', teardown)


def init_opt(Corn):
    class NotOptimizedError(Exception):
        pass

    def error():
        raise NotOptimizedError

    Corn._all = error
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()


@suite.test
def test_opt_base(Corn):
    init_opt(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.name == 'foo').execute())
    assert len(items) == 2
    assert all(item['name'] == 'foo' for item in items)
    items = list(Corn.all.filter((c.name == 'foo') &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    items = list(Corn.all.filter((c.name == 'baz') |
        (c.lastname == 'baz')).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    items = list(Corn.all.filter(c.name == 'foo').execute())
    assert len(items) == 2
    assert all(item['name'] == 'foo' for item in items)


@suite.test
def test_opt_filter_ops(Corn):
    init_opt(Corn)
    items = list(Corn.all.filter((c.name == 'foo') &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter((c.name == 'baz') |
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
    assert all([item.corn == Corn for item in items])


@suite.test
def test_opt_map(Corn):
    init_opt(Corn)
    items = list(Corn.all.map(c.name).execute())
    assert len(items) == 3
    assert all(type(item) == unicode for item in items)
    items = list(Corn.all.map({'foo': c.name}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all('foo' in item for item in items)
    items = list(Corn.all.map(
        {'foo': c.name}).filter(c.foo == 'baz').execute())
    assert len(items) == 1
    assert all(type(item) == dict for item in items)
    assert all(item['foo'] == 'baz' for item in items)
    items = list(Corn.all.map(
        c + {'doubleid': c.id}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all(item['doubleid'] == item['id'] for item in items)
    items = list(Corn.all.map(
        c + {'square': c.id * c.id}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all(item['square'] == item['id'] ** 2 for item in items)
    items = list(Corn.all.map(c + c).execute())
    assert len(items) == 3


@suite.test
def test_opt_subrequest(Corn):
    init_opt(Corn)
    items = list(Corn.all.map(
        c + Corn.all.filter(c.id == c(-1).id).map({
            'otherid': c.id,
            'othername': c.name,
            'otherlastname': c.lastname}).one()).execute())
    assert all(item['id'] == item['otherid'] for item in items)
    items = list(Corn.all.map(
        c + {'foreign': Corn.all.filter(c.id == c(-1).id).one()}).execute())
    assert len(items) == 3
    assert all(hasattr(item['foreign'], 'corn') for item in items)
    assert all(item['foreign']['id'] == item['id'] for item in items)
    items = list(Corn.all.map(
        c + {'homonymes': Corn.all.filter(c.name == c(-1).name)}).execute())
    assert len(items) == 3
    assert all(all(subitem['name'] == item['name']
        for subitem in item['homonymes'])
            for item in items)


@suite.test
def test_opt_sort(Corn):
    init_opt(Corn)
    items = list(Corn.all.sort(c.name).execute())
    assert [item['name'] for item in items] == ['baz', 'foo', 'foo']
    items = list(Corn.all.sort(-c.name).execute())
    assert [item['name'] for item in items] == ['foo', 'foo', 'baz']
    items = list(Corn.all.sort(-c.name, -c.id).execute())
    assert items[0]['name'] == 'foo' and items[0]['id'] == 3
    assert items[1]['name'] == 'foo' and items[1]['id'] == 1
    assert items[2]['name'] == 'baz' and items[2]['id'] == 2


@suite.test
def test_opt_aggregates(Corn):
    init_opt(Corn)
    length = Corn.all.len().execute()
    assert length == 3
    items = list(
        Corn.all.groupby(c.name, group=c.len()).sort(c.group).execute())
    assert len(items) == 2
    assert items[0]['group'] == 1
    assert items[1]['group'] == 2
    item = Corn.all.map(c.id).sum().execute()
    assert item == 6
    item = Corn.all.map(c.id).max().execute()
    assert item == 3
    item = Corn.all.map(c.id).min().execute()
    assert item == 1
    item = Corn.all.sum(c.id).execute()
    assert item == 6
    item = Corn.all.max(c.id).execute()
    assert item == 3
    item = Corn.all.min(c.id).execute()
    assert item == 1


@suite.test
def test_opt_groupby(Corn):
    init_opt(Corn)
    items = list(
        Corn.all.groupby(c.name, group=c.sum(c.id)).sort(c.group).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz' and items[0]['group'] == 2
    assert items[1]['key'] == 'foo' and items[1]['group'] == 4
    items = list(Corn.all.groupby(c.name,
        max__=c.max(c.id),
        min__=c.min(c.id),
        sum__=c.sum(c.id)).sort(c.key).execute())
    assert len(items) == 2
    assert items[0] == {'max__': 2, 'min__': 2, 'sum__': 2, 'key': 'baz'}
    assert items[1] == {'max__': 3, 'min__': 1, 'sum__': 4, 'key': 'foo'}
    items = list(Corn.all.map(c.name + ' ' + c.lastname).execute())
    assert len(items) == 3
    items = list(Corn.all.map(c.name + ' ' + c.lastname).execute())
    assert len(items) == 3
    items = list(Corn.all.map(c.name.upper()).sort(c).execute())
    assert items == ['BAZ', 'FOO', 'FOO']
    items = list(Corn.all.map(c.name.upper().lower()).sort(c).execute())
    assert items == ['baz', 'foo', 'foo']
    items = list(Corn.all.groupby(c.name.upper(),
        max__=c.max(c.id),
        min__=c.min(c.id),
        sum__=c.sum(c.id)).sort(c.key).execute())
    assert len(items) == 2
    assert items[0] == {'max__': 2, 'min__': 2, 'sum__': 2, 'key': 'BAZ'}
    assert items[1] == {'max__': 3, 'min__': 1, 'sum__': 4, 'key': 'FOO'}


@suite.test
def test_opt_slice(Corn):
    init_opt(Corn)
    items = list(Corn.all.map(c.id).sort(c)[1:].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[:2].execute())
    assert items == [1, 2]
    items = list(Corn.all.map(c.id).sort(c)[1:2].execute())
    assert items == [2]
    items = list(Corn.all.map(c.id).sort(c)[-2:].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[:-2].execute())
    assert items == [1]
    items = list(Corn.all.map(c.id).sort(c)[-2:3].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[-2:3].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[1:-1].execute())
    assert items == [2]
    items = list(Corn.all.map(c.id).sort(c)[-2:-1].execute())
    assert items == [2]
    max = Corn.all.sort(c.id)[:2].max(c.id).execute()
    assert max == 2
    min = Corn.all.sort(c.id)[1:].min(c.id).execute()
    assert min == 2
    sum = Corn.all.sort(c.id)[1:2].sum(c.id).execute()
    assert sum == 2


@suite.test
def test_opt_regex(Corn):
    init_opt(Corn)
    items = list(Corn.all.map(c.id.str()).sort().execute())
    assert items == [u'1', u'2', u'3']
    item = Corn.all.map(c.id.str() + (c.id * c.id).str()).filter(
        c.matches("1$")).one().execute()
    assert item == '11'
    items = list(Corn.all.filter(c.name.matches("b.*")).execute())
    assert len(items) == 1
    assert items[0]["id"] == 2
    items = list(Corn.all.filter(c.lastname.matches("^ba\w+$")).execute())
    items2 = list(Corn.all.filter(c.lastname.matches("ba[a-z]")).execute())
    assert len(items) == 3
    assert items == items2
    items = list(Corn.all.filter(c.lastname.matches("a")).execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.lastname.matches(c.name)).execute())
    assert len(items) == 0
    items = list(Corn.all.filter(c.name.matches(c.name)).execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.lastname.matches("\d+")).execute())
    assert len(items) == 0
