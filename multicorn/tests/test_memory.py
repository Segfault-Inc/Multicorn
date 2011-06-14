from attest import Tests, assert_hook

from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.requests.types import Type, Dict, List
from multicorn.requests.wrappers import RequestWrapper

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
