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
Alchemy test
============

Test the alchemy backend on an sqlite base.

"""

from nose.tools import eq_, nottest
from kalamar.access_point.alchemy import AlchemyProperty, Alchemy
from kalamar.site import Site

url = "sqlite:///"


@nottest
def make_testtable():
    id_property = AlchemyProperty(int, column_name="id")
    label = AlchemyProperty(unicode, column_name="label")
    ap = Alchemy(url, "test", {"id": id_property, "label": label}, "id", True)
    return ap


class TestAlchemy:
    def setUp(self):
        self.site = Site()
        self.site.register("test", make_testtable())
        self.items = []
        item = self.site.create("test", {"id": 1, "label": u"Test"})
        self.items.append(item)
        item.save()
        item = self.site.create("test", {"id": 2, "label": u"Test2"})
        self.items.append(item)
        item.save()

    def testsearch(self):
        items = list(self.site.search("test"))
        eq_(len(items), 2)
        items = list(self.site.search("test", {"id": 1}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item["id"], 1)
        eq_(item["label"], "Test")

    def testview(self):
        items = list(
            self.site.view("test", {"truc": "id", "name": u"label"}, {}))
        eq_(len(items), 2)
        for item in items:
            assert "truc" in item.keys() and "name" in item.keys()
        items = list(
            self.site.view("test", {"truc": "id", "name": u"label"}, {"id": 1}))
        eq_(len(items), 1)

    def testupdate(self):
       item = self.site.open("test", {"id": 1})
       item["label"] = u"updated"
       item.save()
       item = self.site.open("test", {"id": 1})
       eq_(item["label"], u"updated")
    
    def tearDown(self):
        for item in self.items:
            item.delete()
        for ap in self.site.access_points.values():
            ap._table.drop()
