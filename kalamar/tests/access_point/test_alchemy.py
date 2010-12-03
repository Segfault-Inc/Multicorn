# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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
Alchemy test.

Test the alchemy backend on an sqlite base.

"""

import unittest
from nose.tools import eq_

from kalamar.access_point.alchemy import AlchemyProperty, Alchemy
from kalamar.site import Site
from ..common import make_site, run_common


def make_ap():
    """Create a simple Alchemy access point."""
    id_property = AlchemyProperty(int, column_name="id")
    name = AlchemyProperty(unicode, column_name="name")
    access_point = Alchemy(
        "sqlite:///", "test", {"id": id_property, "name": name},
        ["id"], True)
    return access_point


class TestAlchemy(unittest.TestCase):
    """Class defining some simple tests on an Alchemy access point."""
    def test_search(self):
        """Test a simple search on the access point."""
        items = list(self.site.search("test"))
        eq_(len(items), 2)
        items = list(self.site.search("test", {"id": 1}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item["id"], 1)
        eq_(item["name"], "Test")

    def test_view(self):
        """Test a simple view on the access point."""
        items = list(
            self.site.view("test", {"truc": "id", "name": u"name"}, {}))
        eq_(len(items), 2)
        for item in items:
            assert "truc" in item.keys() and "name" in item.keys()
        items = list(
            self.site.view("test", {"truc": "id", "name": u"name"}, {"id": 1}))
        eq_(len(items), 1)

    def test_update(self):
        """Assert that an item can be updated in the DB."""
        item = self.site.open("test", {"id": 1})
        item["name"] = u"updated"
        item.save()
        item = self.site.open("test", {"id": 1})
        eq_(item["name"], u"updated")

    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        self.site = Site()
        self.site.register("test", make_ap())
        self.items = []
        item = self.site.create("test", {"id": 1, "name": u"Test"})
        self.items.append(item)
        item.save()
        item = self.site.create("test", {"id": 2, "name": u"Test2"})
        self.items.append(item)
        item.save()

    def tearDown(self):
        for item in self.items:
            item.delete()
        for access_point in self.site.access_points.values():
            access_point._table.drop()
        Alchemy.__metadatas = {}
    # pylint: enable=C0103


# Common tests

def runner(test):
    """Test runner for ``test``."""
    access_point = make_ap()
    try:
        site = make_site(access_point,
            fill=not hasattr(test, "nofill"))
        test(site)
    finally:
        access_point._table.drop()
        Alchemy.__metadatas = {}

@run_common
def test_alchemy_common():
    """Define a custom test runner for the common tests."""
    return make_ap(), runner
