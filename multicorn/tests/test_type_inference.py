from attest import Tests

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
    return Corn



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
    assert List(Corn.type) == type

@suite.test
def test_filter():
    Corn = make_corn()
    type = return_type(Corn.all.filter(c.name == 'lol'))
    assert List(Corn.type) == type

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
        'grou': Type(int)}))
    type = return_type(Corn.all.one())
    assert type == Dict(corn=Corn, mapping=Corn.properties)
    type = return_type(Corn.all.map({
        'name': c.name + c.lastname,
        'test': c.len() + 10}).one())
    assert type == Dict(mapping={
        'name': Type(unicode),
        'test': Type(int)})
    type = return_type(Corn.all.map({
        'max': Corn.all.max(),
        'min': Corn.all.min(),
        'len': Corn.all.len(),
        'sum': Corn.all.sum(),
        'distinct': Corn.all.distinct(),
        'bool': c.name == c.lastname,
        'otherbool': ~(c.id < 3),
        'andor': (c.id < 3) | (c.name.len() < 2) & (c.lastname + "test"),
        'add': Corn.all.map(c.name + c.lastname),
        'sub': Corn.all.map(c.name + c.lastname),
        'mul': Corn.all.map(c.id * c.id),
        'div': Corn.all.map(c.id / c.id),
        'index': Corn.all[10],
        'slice': Corn.all[10:20],
        'one': Corn.all.one(),
        'onedefault': Corn.all.one(None),
        'onedefaulthomogeneous': Corn.all.one(Corn.all.one()),
        'sort': Corn.all.sort(c.name, ~c.lastname),
        'groupby': Corn.all.groupby(c.name),
        'heterogeneous_list': Corn.all + ['toto', 3],
        'homogeneous_list': [u'toto', u'tata'],
        'tuple': (1, 2, 3)

        }))
    assert isinstance(type, List)
    mapping = type.inner_type.mapping
    for key in ('max', 'min', 'sum', 'one', 'index'):
        assert mapping[key] == Corn.type
    for key in ('len',):
        assert mapping[key] == Type(int)
    for key in ('bool', 'otherbool', 'andor'):
        assert mapping[key] == Type(bool)
    for key in ('distinct', 'slice', 'sort'):
        assert mapping[key] == List(Corn.type)
    assert mapping['groupby'] == List(Dict(mapping={
        'grouper': Corn.properties['name'],
        'elements': List(Corn.type)}))
    assert mapping['heterogeneous_list'] == List(Type(object))
    assert mapping['homogeneous_list'] == List(Type(unicode))
    assert mapping['tuple'] == Type(tuple)
    assert mapping['onedefaulthomogeneous'] == Corn.type
    assert mapping['onedefault'] == Type(object)
