# coding: utf8
"""Common tests run against all access points"""
from nose.tools import eq_, raises
from kalamar import MultipleMatchingItems, ItemDoesNotExist

from kalamar.tests.common import nofill, commontest


@nofill
@commontest
def test_single_item(site):
    """Save a single item and retrieve it."""
    site.create("things", {"id": 1, "name": u"foo"}).save()
    all_items = list(site.search("things"))
    eq_(len(all_items), 1)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "foo")

@commontest
def test_search(site):
    """Test a simple search"""
    results = site.search("things", {"name": u"bar"})
    eq_(set(item["id"] for item in results), set([2, 3]))

@commontest
def test_open_one(site):
    """Test a standard "open"""
    result = site.open("things", {"name": u"foo"})
    eq_(result["id"], 1)

@commontest
@raises(MultipleMatchingItems)
def test_open_two(site):
    """An exception is raised when muliple items match an open request"""
    site.open("things", {"name": u"bar"})

@commontest
@raises(ItemDoesNotExist)
def test_open_zero(site):
    """An exception is raised when no item match an open request"""
    site.open("things", {"name": u"nonexistent"})

@commontest
def test_delete(site):
    """Test a simple delete"""
    item = site.open("things", {"name": u"foo"})
    item.delete()
    eq_(list(site.search("things", {"name": u"foo"})), [])

@commontest
def test_delete_many(site):
    """Test a multiple delete"""
    site.delete_many("things", {"name": u"bar"})
    eq_(list(site.search("things", {"name": u"bar"})), [])

@commontest
def test_eq(site):
    """Test equality between items"""
    item1 = site.open("things", {"name": u"foo"})
    item2 = site.open("things", {"name": u"foo"})
    eq_(item1, item2)

@commontest
def test_view(site):
    """Test simple view request"""
    items = list(site.view("things", {"foo": "name"}))
    eq_(len(items), 3)
    assert(all(["foo" in item for item in items]))

@commontest
def test_view_condition(site):
    """Test simple view condition"""
    items = list(site.view("things", {"name":"name"}, {"name": u"bar"}))
    eq_(len(items), 2)
    assert(all([item["name"] == "bar" for item in items]))
    items = list(site.view("things", {"foo":"name"}, {"name": u"bar"}))
    eq_(len(items), 2)
    assert(all([item["foo"] == "bar" for item in items]))
    items = list(site.view("things", {"foo":"name"}, {"foo": u"bar"}))
    eq_(len(items), 2)
    assert(all([item["foo"] == "bar" for item in items]))

@commontest
def test_view_order_by(site):
    """Test order by argument"""
    items = list(site.view("things", {"name":"name"}, 
        {}, [("name", True)]))
    assert(all([n_1["name"] >= n["name"] 
        for n, n_1 in zip(items, items[1:])]))
    items = list(site.view("things", {"name":"name", "id":"id"}, 
        {}, [("name", True),("id",False)]))
    assert(all([n_1["name"] >= n["name"]
            and n_1["id"] <= n["id"]
        for n, n_1 in zip(items, items[1:])]))

@commontest
def test_view_range(site):
    """Test range argument"""
    items = list(site.view("things", select_range=2))
    eq_(len(items), 2)
    items = list(site.view("things", order_by=[("id", True)], 
        select_range=(1, 2)))
    eq_(len(items), 1)
    eq_(items[0]["id"], 2)

@commontest
def test_view_star_request(site):
    """Test a view with a wildcard"""
    items = list(site.view("things", {"": "*"}))
    eq_(len(items), 3)
    for item in items:
        assert(all([attr in item for attr in ["name","id"]]))
    items = list(site.view("things", {"prefix_": "*"}))
    eq_(len(items), 3)
    for item in items:
        assert(all(["prefix_%s" % attr in item for attr in ["name","id"]]))
