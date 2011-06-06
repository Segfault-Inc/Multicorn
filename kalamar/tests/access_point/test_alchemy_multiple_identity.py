# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Alchemy test with multiple identities.

Test the alchemy backend on an sqlite base with multiple primary keys.

"""

# TODO: write interesting functions really testing multiple identities

import unittest
from datetime import date
from nose.tools import eq_

from kalamar.access_point.alchemy import AlchemyProperty, Alchemy
from kalamar.site import Site
from ..common import require



def make_table():
    """Return an access point with multiple identities."""
    Alchemy.__metadatas = {}
    firstname = AlchemyProperty(unicode, column_name="firstname")
    lastname = AlchemyProperty(unicode, column_name="lastname")
    birthdate = AlchemyProperty(date, column_name="birthdate")
    access_point = Alchemy(
        "sqlite:///", "test_multiple_keys", {
            "firstname": firstname,
            "lastname": lastname,
            "birthdate": birthdate},
        ["firstname", "lastname"], True)
    # Accessing access_point._table calls access_point._table()
    # pylint: disable=W0104
    access_point._table
    # pylint: enable=W0104
    return access_point


@require("sqlalchemy")
class TestAlchemy(unittest.TestCase):
    """Class defining some simple tests on an Alchemy access point."""
    def test_search(self):
        """Test a simple search on the access point."""
        items = list(self.site.search("test"))
        eq_(len(items), 2)
        items = list(self.site.search("test", {"firstname": "John"}))
        eq_(len(items), 1)
        item = items[0]
        eq_(item["firstname"], "John")
        eq_(item["lastname"], "Doe")
        eq_(item["birthdate"], date(1950, 1, 1))

    def test_view(self):
        """Test a simple view on the access point."""
        items = list(
            self.site.view(
                "test", {"truc": "firstname", "name": "lastname"}, {}))
        eq_(len(items), 2)
        for item in items:
            assert "truc" in item.keys() and "name" in item.keys()
        items = list(
            self.site.view("test", {"truc": "firstname", "name": "lastname"},
                {"firstname": "John"}))
        eq_(len(items), 1)

    def test_update(self):
        """Assert that an item can be updated in the DB."""
        item = self.site.open(
            "test", {"firstname": "John", "lastname": "Doe"})
        item["birthdate"] = date(1951, 12, 12)
        item.save()
        item = self.site.open(
            "test", {"firstname": "John", "lastname": "Doe"})
        eq_(item["birthdate"],  date(1951, 12, 12))

    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        self.site = Site()
        self.access_point = make_table()
        self.site.register("test", self.access_point) 
        self.items = []
        item = self.site.create(
            "test", {"firstname": "John", "lastname": "Doe",
                     "birthdate": date(1950, 1, 1)})
        self.items.append(item)
        item.save()
        item = self.site.create(
            "test", {"firstname": "Jane", "lastname": "Doe",
                     "birthdate": date(1960, 2, 2)})
        self.items.append(item)
        item.save()

    def tearDown(self):
        for access_point in self.site.access_points.values(): 
            access_point._table.drop()
    # pylint: enable=C0103
