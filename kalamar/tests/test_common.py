# coding: utf8
from nose.tools import eq_, nottest, raises
from kalamar import MultipleMatchingItems, ItemDoesNotExist

from kalamar.tests.common import nofill, commontest

@nofill
@commontest
def test_single_item(site):
    """Save a single item and retrieve it."""
    site.create("things", {"id": 1, "name": "foo"}).save()
    all_items = list(site.search("things"))
    eq_(len(all_items), 1)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "foo")

@commontest
def test_search(site):
    results = site.search("things", {"name": "bar"})
    eq_(set(item["id"] for item in results), set([2, 3]))

@commontest
def test_open_one(site):
    result = site.open("things", {"name": "foo"})
    eq_(result["id"], 1)

@commontest
@raises(MultipleMatchingItems)
def test_open_two(site):
    result = site.open("things", {"name": "bar"})

@commontest
@raises(ItemDoesNotExist)
def test_open_zero(site):
    result = site.open("things", {"name": "nonexistent"})

@commontest
def test_delete(site):
    item = site.open("things", {"name": "foo"})
    item.delete()
    eq_(list(site.search("things", {"name": "foo"})), [])

@commontest
def test_delete_many(site):
    site.delete_many("things", {"name": "bar"})
    eq_(list(site.search("things", {"name": "bar"})), [])

