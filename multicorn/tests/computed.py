from attest import assert_hook
import attest

from multicorn.corns.memory import Memory
from multicorn import Multicorn
from multicorn.declarative import declare, Property
from multicorn.requests import CONTEXT as c
from multicorn.corns.extensers.computed import ComputedExtenser

def make_corn():
    mc = Multicorn()
    @declare(Memory, identity_properties=("id",))
    class Corn(object):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
    extenser = ComputedExtenser("Corn", Corn)
    extenser.register("fullname", c.name + ' : ' + c.lastname)
    extenser.register("homonymes", extenser.all.filter((c(-1).name == c.name) &
                                    (c(-1).id != c.id)))
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    mc.register(extenser)
    return extenser

suite = attest.Tests()


@suite.test
def test_basic():
    Corn = make_corn()
    items = list(Corn.all.execute())
    assert len(items)
    for item in items:
        assert item['fullname'] == item['name'] + ' : ' + item['lastname']
        assert all(subitem['name'] == item['name'] for subitem in item['homonymes'])
    item = Corn.all.filter(c.fullname == 'foo : bar').one().execute()
    assert item['id'] == 1
    assert item['fullname'] == 'foo : bar'
    homonymes = list(item['homonymes'])
    assert len(homonymes) == 1
    assert homonymes[0]['id'] == 3
    assert homonymes[0]['fullname'] == 'foo : baz'
    double_homonymes = list(homonymes[0]['homonymes'])
    assert len(double_homonymes) == 1
    assert double_homonymes[0]['fullname'] == 'foo : bar'
    items = list(Corn.all.map({'myself': c.id, 'others': c.homonymes.map({
        'name': c.fullname, 'others': c.homonymes.map(c.id)})})
        .sort(c.myself).execute())
    assert len(items) == 3
    foobar = items[0]
    assert foobar['myself'] == 1
    others = list(foobar['others'])
    assert len(others) == 1
    assert others[0]['name'] == 'foo : baz'
    assert list(others[0]['others']) == [1]
