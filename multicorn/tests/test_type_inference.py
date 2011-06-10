from attest import Tests, assert_hook
import attest

from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests.types import Type, Dict, List
from multicorn.requests.wrappers import RequestWrapper
from multicorn im

suite = Tests()

def make_corn():
    mc = Multicorn()
    @mc.register
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(int)
        name = Property(unicode)
    return Corn


def expected_type(Corn):
    return List(inner_type=Dict(corn=Corn, mapping=Corn.properties))

@suite.test
def test_simple_types():
    Corn = make_corn()
    request = Corn.all
    type = RequestWrapper.from_request(request).return_type()
    assert isinstance(type, List)
    assert isinstance(type.inner_type, Dict)
    item_def = type.inner_type.mapping
    assert 'id' in item_def
    for key in ('id', 'name'):
        assert isinstance(item_def[key], Type)
        assert item_def[key].corn == Corn
        assert item_def[key].name == key

@suite.test
def test_filter():
    Corn = make_corn()
    #  TODO: test this








