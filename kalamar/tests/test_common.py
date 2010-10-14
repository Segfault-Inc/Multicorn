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


