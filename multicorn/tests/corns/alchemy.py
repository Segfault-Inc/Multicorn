# -*- coding: utf-8 -*-
from attest import assert_hook
import attest
from multicorn import Multicorn
from multicorn.corns.alchemy import Alchemy
from multicorn.declarative import declare, Property
from . import make_test_suite
from multicorn.requests import CONTEXT as c

from psycopg2.extensions import STATUS_BEGIN


def make_corn():
    @declare(Alchemy, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


def make_postgres_corn():
    @declare(Alchemy, identity_properties=("id",),
            url="postgresql://multicorn:multicorn@localhost/",
            engine_opts={'strategy': 'threadlocal'})
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


def teardown(Corn):
    Corn.table.drop()

emptysuite, fullsuite = make_test_suite(
    make_corn, 'alchemy-sqlite', teardown=teardown)
postgres_emptysuite, postgres_fullsuite = make_test_suite(
    make_postgres_corn, 'alchemy-postgres', teardown=teardown)


@emptysuite.test
@postgres_emptysuite.test
def test_optimization(Corn, data):
    class NotOptimizedError(Exception):
        pass

    def error():
        raise NotOptimizedError

    Corn._all = error
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    item = Corn.all.filter(1 == c.id / 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'

    item = Corn.all.filter((c.id * c.id) == 9).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'

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
    items = list(Corn.all.map(c.name).execute())
    assert len(items) == 3
    assert all(type(item) == unicode for item in items)
    items = list(Corn.all.map({'foo': c.name}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all('foo' in item for item in items)
    items = list(Corn.all.map({'foo': c.name})
                 .filter(c.foo == 'baz').execute())
    assert len(items) == 1
    assert all(type(item) == dict for item in items)
    assert all(item['foo'] == 'baz' for item in items)
    items = list(Corn.all.map(c + {'doubleid': c.id}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all(item['doubleid'] == item['id'] for item in items)
    items = list(Corn.all.map(c + {'square': c.id * c.id}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all(item['square'] == item['id'] ** 2 for item in items)
    items = list(Corn.all.map(c + c).execute())
    assert len(items) == 3
    items = list(Corn.all.map(c + Corn.all.filter(c.id == c(-1).id).map({
            'otherid': c.id, 'othername': c.name, 'otherlastname': c.lastname})
                              .one()).execute())
    assert all(item['id'] == item['otherid'] for item in items)
    req = Corn.all.map(c + {'foreign': Corn.all.filter(c.id == c(-1).id)
                            .one()})
    items = list(req.execute())
    assert len(items) == 3
    assert all(hasattr(item['foreign'], 'corn') for item in items)
    assert all(item['foreign']['id'] == item['id'] for item in items)
    items = list(Corn.all.sort(c.name).execute())
    assert [item['name'] for item in items] == ['baz', 'foo', 'foo']
    items = list(Corn.all.sort(-c.name).execute())
    assert [item['name'] for item in items] == ['foo', 'foo', 'baz']
    items = list(Corn.all.sort(-c.name, -c.id).execute())
    assert items[0]['name'] == 'foo' and items[0]['id'] == 3
    assert items[1]['name'] == 'foo' and items[1]['id'] == 1
    assert items[2]['name'] == 'baz' and items[2]['id'] == 2
    length = Corn.all.len().execute()
    assert length == 3
    items = list(Corn.all.groupby(c.name, group=c.len()).sort(c.group)
                 .execute())
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
    default = object()
    item = Corn.all.filter(c.id == 1000).one(default).execute()
    assert item == default
    items = list(Corn.all.filter(c.name.upper() == 'FOO').execute())
    assert len(items) == 2
    items = list(Corn.all.groupby(c.name, group=c.sum(c.id)).sort(c.group)
                 .execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz' and items[0]['group'] == 2
    assert items[1]['key'] == 'foo' and items[1]['group'] == 4
    items = list(Corn.all.groupby(c.name, group={
        'max': c.max(c.id),
        'min': c.min(c.id),
        'sum': c.sum(c.id)}).sort(c.key).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz' and items[0]['group'] == {

        'max': 2, 'min': 2, 'sum': 2}
    assert items[1]['key'] == 'foo' and items[1]['group'] == {
        'max': 3, 'min': 1, 'sum': 4}
    items = list(Corn.all.map(c.name + ' ' + c.lastname).execute())
    assert len(items) == 3
    items = list(Corn.all.map(c.name.upper()).sort(c).execute())
    assert items == ['BAZ', 'FOO', 'FOO']
    items = list(Corn.all.filter(c.name.matches('f..')).execute())
    assert len(items) == 2
    assert items[0]['name'] == 'foo'
    req = Corn.all.filter(c.id.is_in([1, 2])).sort(c.id)
    items = list(req.execute())
    assert len(items) == 2
    assert items[0]['id'] == 1
    assert items[1]['id'] == 2
    homonymes = Corn.all.map(c + {
        'homonymes': Corn.all.filter(c.name == c(-1).name)})
    items = list(homonymes.execute())
    assert len(items) == 3
    assert all(all(subitem['name'] == item['name']
        for subitem in item['homonymes'])
            for item in items)
    items = list(homonymes.map(c.homonymes).execute())
    assert len(items) == 3
    assert all(hasattr(item, '__iter__') for item in items)
    items = list(items)
    for item in items:
        item = list(item)
        for a in item:
            assert a.corn == Corn


@emptysuite.test
@postgres_emptysuite.test
def test_unicode(Corn, data):
    class NotOptimizedError(Exception):
        pass

    def error():
        raise NotOptimizedError
    Corn._all = error
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'baré'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'barà'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    items = list(Corn.all.map(c + {
        'homonymes': Corn.all.filter(c.name == c(-1).name)}).execute())
    assert len(items) == 3
    assert all(all(subitem['name'] == item['name']
        for subitem in item['homonymes'])
            for item in items)


@emptysuite.test
@postgres_emptysuite.test
def test_case(Corn, data):
    from multicorn.requests import case, when

    class NotOptimizedError(Exception):
        pass

    def error():
        raise NotOptimizedError
    Corn._all = error
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()

    items = list(Corn.all.map(
        {'id': c.id,
         'case': case(
             when(c.id < 3, c.name), c.lastname)})
        .sort(c.id)
        .execute())

    assert len(items) == 3
    assert items[0]['case'] == 'foo'
    assert items[1]['case'] == 'baz'
    assert items[2]['case'] == 'baz'

    items = list(Corn.all.map(
        {'id': c.id,
         'case': case(
             when(c.name == 'foo', 'yoo'),
             when(c.name == 'baz', 'yar')
         )})
        .sort(c.id)
        .execute())
    assert len(items) == 3
    assert items[0]['case'] == 'yoo'
    assert items[1]['case'] == 'yar'
    assert items[2]['case'] == 'yoo'

    items = list(Corn.all.map(
        {'id': c.id,
         'case': case(
             when(c.name == 'foo', 12),
         )})
        .sort(c.id)
        .execute())
    assert len(items) == 3
    assert items[0]['case'] == 12
    assert items[1]['case'] == None
    assert items[2]['case'] == 12
