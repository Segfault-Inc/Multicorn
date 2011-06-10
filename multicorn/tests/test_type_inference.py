from attest import Tests, assert_hook
import attest

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
    return Corn


def expected_type(Corn):
    return List(inner_type=Dict(corn=Corn, mapping=Corn.properties))

def return_type(request):
    return RequestWrapper.from_request(request).return_type()

@suite.test
def test_simple_types():
    Corn = make_corn()
    type = return_type(Corn.all)
    assert isinstance(type, List)
    assert isinstance(type.inner_type, Dict)
    item_def = type.inner_type.mapping
    assert 'id' in item_def
    assert expected_type(Corn) == type

@suite.test
def test_filter():
    Corn = make_corn()
    type = return_type(Corn.all.filter(c.name == 'lol'))
    assert expected_type(Corn) == type

@suite.test
def test_map():
    Corn = make_corn()
    type = return_type(Corn.all.map(c.name))
    assert type == List(inner_type=Corn.properties['name'])

@suite.test
def test_groupby():
    Corn = make_corn()
    type = return_type(Corn.all.groupby(c.name).map(c.elements.map(c(-1).grouper)))
    assert type == List(inner_type=List(inner_type=Corn.properties['name']))

@suite.test
def test_nimp():
    Corn = make_corn()
    type = return_type(Corn.all.map({'name': c.name, 'grou': c.id + 5, 'id': c.id}))
    assert type == List(inner_type=Dict(mapping={
        'name': Corn.properties['name'],
        'id': Corn.properties['id'],
        'grou': Type(type=int)}))





