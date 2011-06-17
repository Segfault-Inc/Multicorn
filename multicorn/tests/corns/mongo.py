from multicorn.corns.mongo import MongoCorn
from multicorn.declarative import declare, Property
from . import make_test_suite


def make_corn():
    @declare(MongoCorn, identity_properties=("id",),
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

suite = make_test_suite(make_corn, teardown)
