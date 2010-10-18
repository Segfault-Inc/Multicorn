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
from kalamar.item import Item

from ..common import run_common, make_site



URL = "sqlite:///"




class TestAlchemy(object):
    def setUp(self):
        id_property = AlchemyProperty(int, column_name="id")
        name = AlchemyProperty(unicode, column_name="name")
        aproot = Alchemy(URL, "root", {
            "id": id_property, 
            "name": name},
        "id", True)

        idchild_property = AlchemyProperty(int, column_name="id")
        namechild = AlchemyProperty(unicode, column_name="name")
        root_prop = AlchemyProperty(Item, column_name="root", 
                relation="many-to-one", remote_ap='root', remote_property='id')
        apchild = Alchemy(URL, "child", {"id": idchild_property, "name":
            namechild, "root" : root_prop}, "id", True)

        self.site = Site()
        self.site.register("root", aproot)
        self.site.register("child", apchild)
        
        self.items = []
        item = self.site.create("root", {"id": 1, "name": u"Test"})
        self.items.append(item)
        item.save()
        itemchild = self.site.create("child", {"id" : 1, "name": u'TestChild',
            "root" : item})
        itemchild.save()
        item = self.site.create("root", {"id": 2, "name": u"Test2"})
        self.items.append(item)
        item.save()
        itemchild = self.site.create("child", {"id" : 2, "name": u'TestChild2',
            "root" : item})
        itemchild.save()
 
        self.items.append(itemchild)

    def testsearch(self):
        items = list(self.site.search("root"))
        eq_(len(items), 2)
        items = list(self.site.search("root", {"id": 1}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item["id"], 1)
        eq_(item["name"], "Test")

    def testview(self):
        mapping = {"truc": "id", 
                   "name": u"name", 
                   "rootname" : "root.name"
        }
        items = list(
            self.site.view("child", mapping, {}))
        eq_(len(items), 2)
        for item in items:
            assert all([a in item.keys() for a in ["truc", "name", "rootname"]])
        items = list(self.site.view("child", mapping, {"root.id": 1}))
        eq_(len(items), 1)
        items = list(self.site.view("child", mapping, {"root.id": 2, "id":
            2}))
        eq_(len(items), 1)
        items = list(self.site.view("child", mapping, {"root.id": 2, "id":
            1}))
        eq_(len(items), 0)

    def tearDown(self):
        for item in self.items:
            item.delete()
        for ap in self.site.access_points.values():
            ap._table.drop()
