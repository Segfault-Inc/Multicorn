from attest import Tests, assert_hook

from multicorn.corns.mongo import MongoCorn
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.requests.types import Type, Dict, List
from multicorn.requests.wrappers import RequestWrapper

suite = Tests()


def make_corn():
    mc = Multicorn()

    @mc.register
    @declare(MongoCorn, identity_properties=("_id",),
             hostname="localhost", port=27017,
             database="dbtst", collection="mctest")
    class Corn(object):
        _id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)

    Corn.create({'_id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'_id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'_id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    return Corn


def unmake_corn(Corn):
    Corn.db.drop_collection(Corn.collection)
    assert Corn.all.len().execute() == 0


@suite.test
def test_all():
    Corn = make_corn()
    items = list(Corn.all.execute())
    assert len(items) == 3
    item = Corn.all.filter(c._id == 1).one().execute()
    assert item['_id'] == 1
    assert item['name'] == u'foo'
    assert item['lastname'] == u'bar'
    unmake_corn(Corn)
