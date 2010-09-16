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
Test the Memory access point.
"""

from nose.tools import eq_, nottest, raises
from kalamar import Site, MultipleMatchingItems, ItemDoesNotExist
from kalamar.access_point.memory import Memory

@nottest
def make_test_ap():
    return Memory({'id': int, 'name': str}, 'id')

@nottest
def make_test_site():
    site = Site()
    site.register('things', make_test_ap())
    site.create('things', {'id': 1, 'name': 'foo'}).save()
    site.create('things', {'id': 2, 'name': 'bar'}).save()
    site.create('things', {'id': 3, 'name': 'bar'}).save()
    return site


def test_single_item():
    """Save a single item and retrieve it."""
    site = Site()
    site.register('things', make_test_ap())
    site.create('things', {'id': 1, 'name': 'foo'}).save()
    all_items = list(site.search('things'))
    eq_(len(all_items), 1)
    item = all_items[0]
    eq_(item['id'], 1)
    eq_(item['name'], 'foo')

def test_search():
    site = make_test_site()
    results = site.search('things', {'name': 'bar'})
    eq_(set(item['id'] for item in results), set([2, 3]))

def test_open_one():
    site = make_test_site()
    result = site.open('things', {'name': 'foo'})
    eq_(result['id'], 1)

@raises(MultipleMatchingItems)
def test_open_two():
    site = make_test_site()
    result = site.open('things', {'name': 'bar'})

@raises(ItemDoesNotExist)
def test_open_zero():
    site = make_test_site()
    result = site.open('things', {'name': 'nonexistent'})

