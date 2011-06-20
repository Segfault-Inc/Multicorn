from attest import Tests, assert_hook


TESTS = []

def corntest(fun):
    TESTS.append(fun)
    return fun


@corntest
def test_emptyness(Corn):
    """ Tests if the corn is clean """
    # Assert we are on an empty corn :
    corn_len = Corn.all.len().execute()
    assert corn_len == 0

    items = list(Corn.all.execute())
    assert len(items) == 0


@corntest
def test_save(Corn):
    """ Tests the creation of an element """

    Corn.create({'id': 1, 'name': u'foo', 'lastname': u'bar'}).save()
    Corn.create({'id': 2, 'name': u'baz', 'lastname': u'bar'}).save()
    Corn.create({'id': 3, 'name': u'foo', 'lastname': u'baz'}).save()
    items = list(Corn.all.execute())
    assert len(items) == 3
