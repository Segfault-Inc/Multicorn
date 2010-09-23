# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Memory test
===========

Test the Memory access point.

"""

from nose.tools import eq_, nottest, raises, assert_equal, assert_raises
from kalamar import Site, MultipleMatchingItems, ItemDoesNotExist
from kalamar.access_point.memory import Memory
from kalamar.property import Property
from kalamar.access_point.cache import Cache

from ..common import run_common, make_site

def make_ap():
    return Cache(Memory({"id": Property(int), "name": Property(unicode)}, "id"))

@run_common
def test_cache():
    return make_ap()

def test_cached_data_do_not_need_underlaying_access_point():
    site = make_site(make_ap(), fill=True)
    ap = site.access_points['things']

    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "foo")

    # Monkey patch to disable ap
#    old_search = ap.__class__.__bases__[0].search
#    ap.__class__.__bases__[0].search = None
    old_search = ap.wrapped_ap.search
    ap.wrapped_ap.search = None
    
    # Search one item
    # With no ap, this must work!
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "foo")

    # Restore ap
    ap.wrapped_ap.search = old_search
#    ap.__class__.__bases__[0].search = old_search

    # Update the item
    item["name"] = 'bob'
    site.save("things", item)

    # Monkey patch to disable ap
#    ap.__class__.__bases__[0].search = None
    ap.wrapped_ap.search = None

    # This may fail because cache is invalided and ap is None
    assert_raises(TypeError, site.search, "things")

    # Restore the ap
#    ap.__class__.__bases__[0].search = old_search
    ap.wrapped_ap.search = old_search
    
    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "bob")

    # Remove the ap and search again with cache
    ap.wrapped_ap.search = None
    
    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "bob")
    
    # Restore it
#    ap.wrapped_ap.search = wrapped_ap.

def test_delegate():
    """Test that the delegated class behave correctly"""
    site = make_site(make_ap(), fill=True)

    # Search one item
    all_items = list(site.search("things"))
    eq_(len(all_items), 3)
    item = all_items[0]

    # We previously had issues with item not knowing their access point
    repr(item)
    item.identity
