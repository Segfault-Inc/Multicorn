from attest import assert_hook

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

suite = make_test_suite(make_corn, teardown=teardown)
suite = make_test_suite(make_corn, 'alchemy')
second_suite = make_test_suite(make_postgres_corn, 'alchemy_postgres')

