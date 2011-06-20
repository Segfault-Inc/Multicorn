from multicorn.corns.alchemy import Alchemy
from multicorn.declarative import declare, Property
from . import make_test_suite
from multicorn.requests import CONTEXT as c


def make_corn():
    @declare(Alchemy, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


def teardown(Corn):
    Corn.table.drop()

#suite = make_test_suite(make_corn, teardown=teardown)
suite = make_test_suite(make_corn, 'alchemy')


@suite.test
def test_optimization(Corn):
    class NotOptimizedError(Exception):
        pass

    def error():
        raise NotOptimizedError

    Corn._all = error
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    items = list(Corn.all.execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.name == 'foo' ).execute())
    assert len(items) == 2
    assert all(item['name'] == 'foo' for item in items)
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
    assert all(item['name'] == 'foo' for item in items)
    items = list(Corn.all.filter((c.name == 'foo' ) &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.filter((c.name == 'baz' ) |
        (c.lastname == 'baz')).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.filter(c.id < 2).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.filter((c.id < 3) & (c.id > 1)).execute())
    assert len(items) == 1
    assert items[0]['id'] == 2
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.filter(c.id >= 2).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.filter(c.id <= 2).execute())
    assert len(items) == 2
    assert 1 in (x['id'] for x in items)
    assert 2 in (x['id'] for x in items)
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.filter(c.id != 2).execute())
    assert len(items) == 2
    assert 1 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all(item.corn == Corn for item in items)
    items = list(Corn.all.map(c.name).execute())
    assert len(items) == 3
    assert all(type(item) == unicode for item in items)
    items = list(Corn.all.map({'foo': c.name}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all('foo' in item for item in items)
    items = list(Corn.all.map({'foo': c.name}).filter(c.foo == 'baz').execute())
    assert len(items) == 1
    assert all(type(item) == dict for item in items)
    assert all(item['foo'] == 'baz' for item in items)
    items = list(Corn.all.map(c + {'doubleid' : c.id}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all(item['doubleid'] == item['id'] for item in items)
    items = list(Corn.all.map(c + {'square' : c.id * c.id}).execute())
    assert len(items) == 3
    assert all(type(item) == dict for item in items)
    assert all(item['square'] == item['id'] ** 2 for item in items)
    items = list(Corn.all.map(c + c).execute())
    assert len(items) == 3
    items = list(Corn.all.map(c + Corn.all.filter(c.id == c(-1).id).map({
            'otherid': c.id, 'othername': c.name, 'otherlastname': c.lastname}).one()).execute())
    assert all(item['id'] == item['otherid'] for item in items)
    items = list(Corn.all.map(c + Corn.all.filter(c.name == c(-1).name).map({
            'otherid': c.id, 'othername': c.name, 'otherlastname': c.lastname}).one()).execute())
    assert len(items) == 5
    assert all(item['name'] == item['othername'] for item in items)
    items = list(Corn.all.map(c + {'foreign': Corn.all.filter(c.id == c(-1).id).one()}).execute())
    assert len(items) == 3
    assert all(hasattr(item['foreign'], 'corn') for item in items)
    assert all(item['foreign']['id'] == item['id'] for item in items)
