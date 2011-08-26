# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from attest import assert_hook
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
    assert item.corn is Corn  # This is an actual Item, not a dict.

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


@corntest
def basic_query(Corn):
    """ Tests basic querying """
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.id == c.id).execute())
    assert len(items) == 3

    item = Corn.all.filter(c.id == 2).one().execute()
    item2 = Corn.all.filter(2 == c.id).one().execute()
    assert item == item2

    items = list(Corn.all.filter(
        c.name + "<" + c.lastname + ">" == "foo<bar>").execute())
    assert len(items) == 1
    item = items[0]
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'

    items = list(Corn.all.filter(
        c.name + u"«" + c.lastname + u"»" == u"foo«bar»").execute())
    assert len(items) == 1
    item = items[0]
    assert item['id'] == 1
    assert item['name'] == 'foo'
    assert item['lastname'] == 'bar'

    item = Corn.all.filter(c.id - 1 == 2).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'

    item = Corn.all.filter((c.id * c.id) == 9).one().execute()
    assert item['id'] == 3
    assert item['name'] == 'foo'
    assert item['lastname'] == 'baz'

    item = Corn.all.filter(1 == c.id / 2).one().execute()
    assert item['id'] == 2
    assert item['name'] == 'baz'
    assert item['lastname'] == 'bar'


@corntest
def maps(Corn):
    """Test various map operations"""
    make_data(Corn)
    items = list(Corn.all.map(c.id).execute())
    assert len(items) == 3
    test = all([x in items for x in [1, 2, 3]])
    assert test
    items = list(Corn.all.map(c.id * c.id).execute())
    assert len(items) == 3
    test = all([x in items for x in [1, 4, 9]])
    assert test
    items = list(Corn.all.map(c.name + ' ' + c.lastname).execute())
    assert len(items) == 3
    test = all([x in items for x in ['foo bar', 'baz bar', 'foo baz']])
    assert test
    item = Corn.all.map(
        c.name + ' ' + c.lastname).filter(c == 'foo bar').one().execute()
    assert item == 'foo bar'
    item = Corn.all.map(
        c.name + u'ùþß' + c.lastname).filter(c == u'fooùþßbar').one().execute()
    assert item == u'fooùþßbar'
    items = list(Corn.all.map(
        {'foo': c.name}).filter(c.foo == 'baz').execute())
    assert len(items) == 1
    assert all(type(item) == dict for item in items)
    assert all(
        item['foo'] == 'baz' for item in items)
    fullname = c.name + ' ' + c.lastname
    item = Corn.all.map(fullname).filter(c == 'foo bar').one().execute()
    assert item == 'foo bar'
    item = Corn.all.map({'fullname': fullname, 'id': c.id}).filter(
            (c.id * c.id) == 1).one().execute()
    assert item == {'fullname': 'foo bar', 'id': 1}


@corntest
def aggregates(Corn):
    """Test aggregates"""
    make_data(Corn)
    max = Corn.all.max(c.id).execute()
    assert max == 3
    min = Corn.all.min(c.id).execute()
    assert min == 1
    len = Corn.all.len().execute()
    assert len == 3


@corntest
def filter(Corn):
    """Test various filters"""
    make_data(Corn)
    items = list(Corn.all.execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.name == 'foo').execute())
    assert len(items) == 2
    assert all([item['name'] == 'foo' for item in items])
    items = list(Corn.all.filter((c.name == 'foo') &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    items = list(Corn.all.filter((c.name == 'baz') |
        (c.lastname == 'baz')).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    items = list(Corn.all.filter(c.name == 'foo').execute())
    assert len(items) == 2
    assert all([item['name'] == 'foo' for item in items])
    items = list(Corn.all.filter((c.name == 'foo') &
        (c.lastname == 'bar')).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter((c.name == 'baz') |
        (c.lastname == 'baz')).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id < 2).execute())
    assert len(items) == 1
    assert items[0]['id'] == 1
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter((c.id < 3) & (c.id > 1)).execute())
    assert len(items) == 1
    assert items[0]['id'] == 2
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id >= 2).execute())
    assert len(items) == 2
    assert 2 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id <= 2).execute())
    assert len(items) == 2
    assert 1 in (x['id'] for x in items)
    assert 2 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])
    items = list(Corn.all.filter(c.id != 2).execute())
    assert len(items) == 2
    assert 1 in (x['id'] for x in items)
    assert 3 in (x['id'] for x in items)
    assert all([item.corn == Corn for item in items])


@corntest
def test_groupby(Corn):
    """Test groupby requests"""
    make_data(Corn)
    items = list(Corn.all.groupby(c.name).sort(c.key).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz'
    assert items[1]['key'] == 'foo'
    elems = list(items[0]['elements'])
    assert len(elems) == 1
    assert dict(elems[0]) == {
        'id': 2, 'name': u'baz', 'lastname': u'bar'}
    elems = list(items[1]['elements'])
    assert len(elems) == 2
    assert {
        'id': 1,
        'name': u'foo',
        'lastname': u'bar'} in (dict(elem) for elem in elems)
    assert {
        'id': 3,
        'name': u'foo',
        'lastname': u'baz'} in (dict(elem) for elem in elems)
    items = list(Corn.all.map(c.name).groupby(c, count=c.len()).sort(c.key).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz'
    assert items[0]['count'] == 1
    assert items[1]['key'] == 'foo'
    assert items[1]['count'] == 2
    items = list(Corn.all.groupby(c.name, max_=c.max(c.id), min_=c.min(c.id),
        sum_=c.sum(c.id)).sort(c.key).execute())
    assert len(items) == 2
    assert items[0] == {'max_': 2, 'min_': 2, 'sum_': 2, 'key': 'baz'}
    assert items[1] == {'max_': 3, 'min_': 1, 'sum_': 4, 'key': 'foo'}
    items = list(Corn.all.groupby(
        c.name, sum=c.sum(c.id)).sort(c.sum).execute())
    assert len(items) == 2
    assert items[0]['key'] == 'baz' and items[0]['sum'] == 2
    assert items[1]['key'] == 'foo' and items[1]['sum'] == 4
    items = list(Corn.all.groupby(
        c.name, len=c.len()).sort(c.len).execute())
    assert len(items) == 2
    assert items[0]['len'] == 1
    assert items[1]['len'] == 2
    items = list(Corn.all.groupby(
        c.name, len=c.len()).sort(c.len).map(
        c.key + ' : ' + c.len.str() + ' items').execute())
    assert len(items) == 2
    assert items[0] == 'baz : 1 items'
    assert items[1] == 'foo : 2 items'


@corntest
def test_str_funs(Corn):
    """Test various string functions"""
    make_data(Corn)
    items = list(Corn.all.map(c.id.str()).sort().execute())
    assert items == [u'1', u'2', u'3']
    item = Corn.all.map(c.id.str() + (c.id * c.id).str()).filter(
        c.matches("1$")).one().execute()
    assert item == '11'
    items = list(Corn.all.map({'login': c.name[1] + c.lastname[1:-1]}).filter(
        c.login == "oa").execute())
    assert len(items) == 2
    items = list(
        Corn.all.map({'first': c.name[:-2]}).filter(c.first == "f").execute())
    assert len(items) == 2
    items2 = list(
        Corn.all.map({'first': c.name[0]}).filter(c.first == "f").execute())
    assert len(items) == 2
    assert items == items2


@corntest
def test_index(Corn):
    """Test various indexings"""
    make_data(Corn)
    item = Corn.all.sort(c.id)[0].execute()
    assert item["id"] == 1
    assert item["name"] == "foo"
    assert item["lastname"] == "bar"
    item = Corn.all.sort(c.id)[1].execute()
    assert item["id"] == 2
    assert item["name"] == "baz"
    assert item["lastname"] == "bar"
    item = Corn.all.sort(c.id)[2].execute()
    assert item["id"] == 3
    assert item["name"] == "foo"
    assert item["lastname"] == "baz"


@corntest
def test_slice(Corn):
    """Test various slices"""
    make_data(Corn)
    items = list(Corn.all.map(c.id).sort(c)[1:].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[:2].execute())
    assert items == [1, 2]
    items = list(Corn.all.map(c.id).sort(c)[1:2].execute())
    assert items == [2]
    items = list(Corn.all.map(c.id).sort(c)[-2:].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[:-2].execute())
    assert items == [1]
    items = list(Corn.all.map(c.id).sort(c)[-2:3].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[-2:3].execute())
    assert items == [2, 3]
    items = list(Corn.all.map(c.id).sort(c)[1:-1].execute())
    assert items == [2]
    items = list(Corn.all.map(c.id).sort(c)[-2:-1].execute())
    assert items == [2]
    items = list(Corn.all.map(c.id).sort(c)[::2].execute())
    assert items == [1, 3]


@corntest
def test_aggregate_slice(Corn):
    """ Test aggregates with slices"""
    make_data(Corn)
    max = Corn.all.sort(c.id)[:2].max(c.id).execute()
    assert max == 2
    min = Corn.all.sort(c.id)[1:].min(c.id).execute()
    assert min == 2
    sum = Corn.all.sort(c.id)[1:2].sum(c.id).execute()
    assert sum == 2


@corntest
def test_re(Corn):
    """ Test various regexp"""
    make_data(Corn)
    items = list(Corn.all.map(c.id.str()).sort().execute())
    assert items == [u'1', u'2', u'3']
    item = Corn.all.map(c.id.str() + (c.id * c.id).str()).filter(
        c.matches("1$")).one().execute()
    assert item == '11'
    items = list(Corn.all.filter(c.name.matches("b.*")).execute())
    assert len(items) == 1
    assert items[0]["id"] == 2
    items = list(Corn.all.filter(c.lastname.matches("^ba\w+$")).execute())
    items2 = list(Corn.all.filter(c.lastname.matches("ba[a-z]")).execute())
    assert len(items) == 3
    assert items == items2
    items = list(Corn.all.filter(c.lastname.matches("a")).execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.lastname.matches(c.name)).execute())
    assert len(items) == 0
    items = list(Corn.all.filter(c.name.matches(c.name)).execute())
    assert len(items) == 3
    items = list(Corn.all.filter(c.lastname.matches("\d+")).execute())
    assert len(items) == 0


@corntest
def test_combinations(Corn):
    """ Test less trivial combinations"""
    make_data(Corn)
    items = list(Corn.all.map(
        {'ln': c.lastname,
         'n': c.name,
         'i': c.id}).map(
        {'i': c.ln,
         'ii': c.n,
         'iii': c.i}).sort(c.iii).execute())
    item = items[0]
    assert item['iii'] == 1
    assert item['ii'] == 'foo'
    assert item['i'] == 'bar'
    item = items[1]
    assert item['iii'] == 2
    assert item['ii'] == 'baz'
    assert item['i'] == 'bar'
    item = items[2]
    assert item['iii'] == 3
    assert item['ii'] == 'foo'
    assert item['i'] == 'baz'
    items = list(Corn.all.filter((c.id < 3) & (c.id > 1)).execute())
    items2 = list(Corn.all.filter(c.id < 3).filter(c.id > 1).execute())
    assert items == items2
    items = list(Corn.all.filter(
        c.name == "foo").map(
        {'idpow': c.id ** c.id}).sort(c.idpow).execute())
    assert items[0]["idpow"] == 1
    assert items[1]["idpow"] == 27

    item = Corn.all.map({
        'fn': c.lastname + ' ' + c.name,
        'ln': c.lastname,
        'n': c.name,
        'i': c.id}).filter(
        c.n == 'foo').map(
        {'fullname': c.fn.upper()[1:], 'index': c.i}).filter(
        c.fullname.matches("^AZ")).one().execute()
    assert item["index"] == 3
    item = (Corn.all.map(c + {'int': 2, 'none': None})
        .filter((c.id == 1) & (c.none == None)).one()
        .execute())
    assert item['int'] == 2
    assert item['none'] == None


@corntest
def test_in_request(Corn):
    make_data(Corn)
    items = list(Corn.all.filter(c.id.is_in([1, 2])).sort(c.id).execute())
    assert len(items) == 2
    assert items[0]['id'] == 1
    assert items[1]['id'] == 2


@corntest
def test_case(Corn):
    from multicorn.requests import case, when
    make_data(Corn)
    assert Corn.all.len() == 3
    items = list(Corn.all.map(
        {'id': c.id,
         'case': case(
             when(c.id < 3, c.name), c.lastname)})
        .sort(c.id)
        .execute())
    assert len(items) == 3
    assert items[0]['case'] == 'foo'
    assert items[1]['case'] == 'baz'
    assert items[2]['case'] == 'baz'

    items = list(Corn.all.map(
        {'id': c.id,
         'case': case(
             when(c.name == 'foo', 'yoo'),
             when(c.name == 'baz', 'yar')
         )})
        .sort(c.id)
        .execute())
    assert len(items) == 3
    assert items[0]['case'] == 'yoo'
    assert items[1]['case'] == 'yar'
    assert items[2]['case'] == 'yoo'

    items = list(Corn.all.map(
        {'id': c.id,
         'case': case(
             when(c.name == 'foo', 12),
         )})
        .sort(c.id)
        .execute())
    assert len(items) == 3
    assert items[0]['case'] == 12
    assert items[1]['case'] == None
    assert items[2]['case'] == 12
