from attest import Tests, assert_hook

from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c

suite = Tests()


def make_corn():
    mc = Multicorn()

    @mc.register
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
def test_all():
    Corn = make_corn()
    items = list(Corn.all.execute())
    assert len(items) == 3
    item = (Corn.all.filter(c.id == 1).one().execute())
    assert item['id'] == 1
    assert item['name'] == u'foo'
    assert item['lastname'] == u'bar'

@suite.test
def test_optimization():
    Corn = make_corn()
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

