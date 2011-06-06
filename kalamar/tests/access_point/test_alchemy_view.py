# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Alchemy view test.

Test the view method with the alchemy backend on an sqlite base.

"""

import unittest
from nose.tools import eq_

from kalamar.access_point.alchemy import AlchemyProperty, Alchemy
from kalamar.site import Site
from kalamar.item import Item
from ..common import require



URL = "sqlite:///"


@require("sqlalchemy")
class TestAlchemy(unittest.TestCase):
    """Class defining some simple ``view`` tests on an Alchemy access point."""
    def test_search(self):
        """Test a simple search on the access point."""
        items = list(self.site.search("root"))
        eq_(len(items), 2)
        items = list(self.site.search("root", {"id": 1}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item["id"], 1)
        eq_(item["name"], "Test")

    def test_view(self):
        """Test a simple view on the access point."""
        mapping = {"truc": "id", "name": "name", "rootname": "root.name"}
        items = list(self.site.view("child", mapping, {}))
        eq_(len(items), 2)
        for item in items:
            assert all(a in item.keys() for a in ["truc", "name", "rootname"])
        items = list(self.site.view("child", mapping, {"root.id": 1}))
        eq_(len(items), 1)
        items = list(self.site.view("child", mapping, {"root.id": 2, "id": 2}))
        eq_(len(items), 1)
        items = list(self.site.view("child", mapping, {"root.id": 2, "id": 1}))
        eq_(len(items), 0)

    def test_star(self):
        """Test a star request on the access point."""
        mapping = {"root_": "root.*"}
        items = list(self.site.view("child", mapping, {}))
        eq_(len(items), 2)
        for item in items:
            assert all(attr in item.keys() for attr in ("root_name", "root_id"))

    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        id_property = AlchemyProperty(int, column_name="id")
        name = AlchemyProperty(unicode, column_name="name")
        aproot = Alchemy(
            URL, "root", {"id": id_property, "name": name}, ["id"], True)

        idchild_property = AlchemyProperty(int, column_name="id")
        namechild = AlchemyProperty(unicode, column_name="name")
        root_prop = AlchemyProperty(
            Item, column_name="root", relation="many-to-one", remote_ap="root",
            remote_property="id")
        apchild = Alchemy(
            URL, "child",
            {"id": idchild_property, "name": namechild, "root": root_prop},
            ["id"], True)

        self.site = Site()
        self.site.register("root", aproot)
        self.site.register("child", apchild)
        
        self.items = []
        item = self.site.create("root", {"id": 1, "name": "Test"})
        self.items.append(item)
        item.save()
        itemchild = self.site.create(
            "child", {"id": 1, "name": "TestChild", "root": item})
        itemchild.save()
        item = self.site.create("root", {"id": 2, "name": "Test2"})
        self.items.append(item)
        item.save()
        itemchild = self.site.create(
            "child", {"id": 2, "name": "TestChild2", "root": item})
        itemchild.save()
 
        self.items.append(itemchild)

    def tearDown(self):
        for access_point in self.site.access_points.values():
            access_point._table.drop()
        Alchemy.__metadatas = {}
    # pylint: enable=C0103
