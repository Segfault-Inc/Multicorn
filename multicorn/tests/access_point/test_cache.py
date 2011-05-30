# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Memory test.

Test the Memory access point.

"""

# Nose redefines assert_raises
# pylint: disable=E0611
from nose.tools import eq_, assert_raises
# pylint: enable=E0611

from multicorn.access_point.cache import Cache
from multicorn.access_point.memory import Memory
from multicorn.property import Property
from multicorn.request import Condition, Not

from ..common import run_common, make_site



def make_ap():
    """Build a simple cache access point."""
    return Cache(Memory(
            {"id": Property(int), "name": Property(unicode)}, ("id",)))

@run_common
def test_cache():
    """Launch common tests for cache."""
    return make_ap()

def test_without_underlying_ap():
    """Assert that the cached data does not need the underlying access point."""
    site = make_site(make_ap(), fill=True)
    access_point = site.access_points["things"]

    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    (item,) = (item for item in all_items if item["id"] == 1)
    eq_(item["name"], "foo")

    # Monkey patch to disable ap
    old_search = access_point.wrapped_ap.search
    access_point.wrapped_ap.search = None
    
    # Search one item
    # With no ap, this must work!
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]
    (item,) = (item for item in all_items if item["id"] == 1)
    eq_(item["name"], "foo")

    # Restore ap
    access_point.wrapped_ap.search = old_search

    # Update the item
    item["name"] = 'bob'
    site.save("things", item)

    # Monkey patch to disable ap
    access_point.wrapped_ap.search = None

    # This may fail because cache is invalided and ap is None
    assert_raises(TypeError, site.search, "things")

    # Restore the ap
    access_point.wrapped_ap.search = old_search
    
    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    (item,) = (item for item in all_items if item["id"] == 1)
    eq_(item["id"], 1)
    eq_(item["name"], "bob")

    # Remove the ap and search again with cache
    access_point.wrapped_ap.search = None
    
    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    (item,) = (item for item in all_items if item["id"] == 1)
    eq_(item["id"], 1)
    eq_(item["name"], "bob")
    
def test_delegate():
    """Test that the delegated class behave correctly."""
    site = make_site(make_ap(), fill=True)

    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]

    # We previously had issues with item not knowing their access point
    # pylint: disable=W0104
    repr(item)
    item.identity
    # pylint: enable=W0104

def test_delete():
    """Deleting an item must flush the cache."""
    site = make_site(make_ap(), fill=True)
    access_point = site.access_points["things"]

    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]
    all_items.remove(item)
    access_point.delete(item)
    eq_(all_items, list(site.search("things")))

def test_delete_many():
    """Deleting many items must flush the cache."""
    site = make_site(make_ap(), fill=True)
    site.delete_many("things", Condition("id", ">=", "2"))
    eq_(list(site.search("things", Not(Condition("id", ">=", "2")))),
        list(site.search("things")))
