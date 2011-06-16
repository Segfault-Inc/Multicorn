from multicorn.corns.memory import Memory
from multicorn.declarative import declare, Property
from . import make_test_suite

def make_corn():
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    return Corn


suite = make_test_suite(make_corn)
