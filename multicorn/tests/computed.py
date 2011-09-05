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
    class Corn(ComputedExtenser):
        id = Property(type=int)
        name = Property(type=unicode)
        lastname = Property(type=unicode)
        bff = Property(type=int)

    Corn.register("fullname", c.name + ' : ' + c.lastname)
    Corn.register("homonymes", Corn.all.filter((c(-1).name == c.name) &
                                    (c(-1).id != c.id)))
    friend_request = Corn.all.filter((c(-1).bff == c.id)\
            & (c(-1).bff != None)).one(None)
    reverse = {'bff': c.friend.id}
    Corn.register("friend", friend_request, reverse=reverse)
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    mc.register(Corn)
    return Corn

suite = attest.Tests()


@suite.test
def test_basic():
    Corn = make_corn()
    items = list(Corn.all.execute())
    assert len(items)
    for item in items:
        assert item['fullname'] == item['name'] + ' : ' + item['lastname']
        assert all([subitem['name'] == item['name']
                    for subitem in item['homonymes']])
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


@suite.test
def test_save():
    Corn = make_corn()
    item_one = Corn.all.filter(c.id == 1).one().execute()
    item_two = Corn.all.filter(c.id == 2).one().execute()
    item_one['friend'] = item_two
    item_one.save()
    item_one = Corn.all.filter(c.id == 1).one().execute()
    friend = item_one['friend']
    assert item_one['friend'] == item_two
