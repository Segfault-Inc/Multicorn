from attest import Tests, assert_hook
from multicorn.requests import CONTEXT as c


TESTS = []

def corntest(fun):
    TESTS.append(fun)
    return fun


def make_data(Corn):
    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()


@corntest
def emptyness(Corn):
    """ Tests if the corn is clean """
    # Assert we are on an empty corn :
    corn_len = Corn.all.len().execute()
    assert corn_len == 0

    items = list(Corn.all.execute())
    assert len(items) == 0


@corntest
def save(Corn):
    """ Tests the creation of an element """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3


@corntest
def existence(Corn):
    """ Tests the existence of created elements """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3

    item = Corn.all.filter(c.id == 1).one().execute()
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'
    assert item.corn is Corn # This is an actual Item, not a dict.

    item = Corn.all.filter(c.id == 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'
    assert item.corn is Corn

    item = Corn.all.filter(c.id == 3).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'
    assert item.corn is Corn

    same_item = Corn.all.filter(c.id == 3).one().execute()
    assert same_item == item
    # The Corn always return new Item instance
    assert same_item is not item


@corntest
def add(Corn):
    """ Tests adding an element """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    Corn.create({'id': 10, 'name': 'ban', 'lastname': 'bal'}).save()
    items = list(Corn.all.filter(c.id == 10).execute())
    assert len(items) == 1
    item = items[0]
    assert item['id'] == 10
    assert item['name'] == 'ban'
    assert item['lastname'] == 'bal'


@corntest
def delete(Corn):
    """ Tests deleting an element """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    item = Corn.all.filter(c.id == 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'
    item.delete()
    items = list(Corn.all.execute())
    assert len(items) == 2
    assert Corn.all.filter(c.id == 1).len().execute() == 1
    assert Corn.all.filter(c.id == 2).len().execute() == 0
    assert Corn.all.filter(c.id == 3).len().execute() == 1
