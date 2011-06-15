from attest import assert_hook, Tests
from attest.collectors import TestBase, test

from multicorn.corns.mongo import MongoCorn
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.requests.types import Type, Dict, List
from multicorn.requests.wrappers import RequestWrapper


class Mongo(TestBase):

    def __context__(self):
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
        self.corn = Corn
        yield
        Corn.db.drop_collection(Corn.collection)

    @test
    def existence(self):
        items = list(self.corn.all.execute())
        assert len(items) == 3
        item = self.corn.all.filter(c._id == 1).one().execute()
        assert item['_id'] == 1
        assert item['name'] == u'foo'
        assert item['lastname'] == u'bar'

    @test
    def add(self):
        items = list(self.corn.all.execute())
        assert len(items) == 3
        item = self.corn.all.filter(c._id == 1).one().execute()
        assert item['_id'] == 1
        assert item['name'] == u'foo'
        assert item['lastname'] == u'bar'

suite = Tests([Mongo()])
