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
View test.

Test the view request algorithm.

"""

import unittest
from nose.tools import eq_

from kalamar.request import Condition, And, Or, Not
from kalamar.access_point.memory import Memory
from kalamar.property import Property
from kalamar.site import Site
from kalamar.query import BadQueryException, QuerySelect, QueryFilter
from kalamar.item import Item



class TestView(unittest.TestCase):
    """Test class testing simple ``view`` requests."""
    def test_lazy(self):
        """Assert that the lazy props (one to many relationships) are set up."""
        root = self.site.open("root", {"id": 0})
        eq_(len(list(root["children"])), 2)

    def test_bad_request(self):
        """Assert that common programmer error raises a BadQueryException."""
        try:
            list(self.site.view("root", {"leaf_label": "children.grou"}, {}))
        except BadQueryException as detail:
            assert(isinstance(detail.query, QuerySelect))
        else:
            assert False, "Expected BadQueryException."

        try:
            list(self.site.view("root", {"leaf_label": "children.label"},
                                {"children.grou": 4}))
        except BadQueryException as detail:
            assert(isinstance(detail.query, (QuerySelect, QueryFilter)))
        else:
            assert False, "Expected BadQueryException."

        try:
            list(self.site.view("root", {"leaf_label": "children.label"},
                                {"children.children.id": "abc"}))
        except BadQueryException as detail:
            assert(isinstance(detail.query, QueryFilter))
        else:
            assert False, "Expected BadQueryException."

        try:
            list(self.site.view("root", {"leaf_label": "childr.label"}))
        except BadQueryException as detail:
            assert(isinstance(detail.query, QuerySelect))
        else:
            assert False, "Expected BadQueryException"

    def test_first_level(self):
        """Assert that the tree can be viewed to the first level."""
        aliases = {"leaf_label": "children.label", "root_label": "label"}
        items = list(self.site.view("root", aliases, {}))
        eq_(len(items), 2)

    def test_star_request(self):
        """Assert that an ``*`` request correctly selects the properties"""
        aliases = {"": "*", "children_": "children.*"}
        items = list(self.site.view("root", aliases, {}))
        eq_(len(items), 2)
        item = items[0]
        assert(all(alias in item for alias
                   in ("label", "id", "children_label", "children_id")))

    def test_star_request_with_cond(self):
        """Assert that ``*``-request-generated alias can be tested in conds."""
        aliases = {"": "*", "children_": "children.*"}
        request = {"label": "root", "children_id": 1}
        items = list(self.site.view("root", aliases, request))
        eq_(len(items), 1)
        item = items[0]
        assert(all(alias in item for alias
                   in ("label", "id", "children_label", "children_id")))

    def test_first_level_with_cond(self):
        """Assert that remote properties can be used in conditions."""
        aliases = {"leaf_label": "children.label", "root_label": "label"}
        conditions = {"children.label": "1"}
        items = list(self.site.view("root", aliases, conditions))
        eq_(len(items), 1)

    def test_leaf_nodes(self):
        """Assert that query involving 2+ chained APs can be executed."""
        aliases = {
            "leaf_label": "children.children.label", "root_label": "label",
            "middle_label": "children.label"}
        condition = Condition("children.children.label", "=", "2.2")
        items = list(self.site.view("root", aliases, condition))
        eq_(len(items), 1)
        assert(all(item["leaf_label"] == "2.2" for item in items))
        condition = Condition("children.label", "=", "2")
        items = list(self.site.view("root", aliases, condition))
        eq_(len(items), 2)
        assert(all(item["middle_label"] == "2" for item in items))
        condition = {"children.label": "2", "children.children.label": "1.1"}
        items = list(self.site.view("root", aliases, condition))
        eq_(len(items), 0)

    def test_and_condition(self):
        """Assert that And conditions can be tested accross multiple APs."""
        aliases = {"root_label": "label", "middle_label": "children.label"}
        condition = And(Condition("children.label", "=", "1"),
                        Condition("children.children.label", "=", "1.1"))
        items = list(self.site.view("root", aliases, condition))
        eq_(len(items), 1)

    def test_or_condition(self):
        """Assert that Or conditions can be tested accross multiple APs."""
        aliases = {"root_label": "label", "middle_label": "children.label"}
        condition = Or(Condition("children.label", "=", "1"),
                       Condition("children.children.label", "=", "2.1"))
        items = list(self.site.view("root", aliases, condition))
        eq_(len(items), 3)

    def test_not_condition(self):
        """Assert that Not conditions can be tested accross multiple APs."""
        aliases = {"root_label": "label", "middle_label": "children.label"}
        condition = Not(Condition("children.children.label", "=", "1.1"))
        items = list(self.site.view("root", aliases, condition))
        eq_(len(items), 3)

    def test_empty_view(self):
        """Assert that ``view`` with props matching nothing return nothing."""
        aliases = {"label": "label"}
        items = list(self.site.view("level2", aliases, {"label": "3"}))
        eq_(len(items), 0)

    def test_parent_property(self):
        """Assert that many-to-one properties works across multiple APs."""
        aliases = {"label": "label", "parent": "parent"}
        items = list(self.site.view("level2", aliases, {"parent.label": "1"}))
        eq_(len(items), 2)
        for item in items:
            eq_(item["parent"].access_point.name, "level1")

    def test_deep_parent_property(self):
        """Assert that deep many-to-one properties works across multiple APs."""
        aliases = {"label": "label", "parent": "parent"}
        items = list(
            self.site.view("level2", aliases, {"parent.parent.label": "root"}))
        eq_(len(items), 4)
        for item in items:
            eq_(item["parent"].access_point.name, "level1")
            eq_(item["parent"]["parent"].access_point.name, "root")

    def test_children_property(self):
        """Assert that one-to-many properties works across multiple APs."""
        aliases = {"label": "label", "children": "children"}
        items = list(
            self.site.view("level1", aliases, {"children.label": "1.1"}))
        eq_(len(items), 1)
        item = items[0]
        eq_(len(item["children"]), 2)
        for child in item["children"]:
            eq_(child.access_point.name, "level2")

    def test_deep_children_property(self):
        """Assert that deep one-to-many properties works across multiple APs."""
        aliases = {"label": "label", "children": "children"}
        items = list(
            self.site.view("root", aliases, {"children.children.label": "1.1"}))
        eq_(len(items), 1)
        item = items[0]
        eq_(len(item["children"]), 2)
        for child in item["children"]:
            eq_(child.access_point.name, "level1")
            grandchildren = child["children"]
            eq_(len(grandchildren), 2)
            for grandchild in grandchildren:
                eq_(grandchild.access_point.name, "level2")

    # camelCase function names come from unittest
    # pylint: disable=C0103
    def setUp(self):
        """Create a test site with 3 access points forming a tree structure.

        The structure looks like this::

            root -> level1 -> level2

        The structure is filled with test data::

            root -> 1 -> 1.1
                      -> 1.2
                 -> 2 -> 2.1
                      -> 2.2

        """
        child_property = Property(
            tuple, relation="one-to-many", remote_ap="level1",
            remote_property="parent")
        root_ap = Memory(
            {"id": Property(int), "label": Property(unicode),
             "children": child_property}, ("id",))

        parent_property = Property(
            Item, relation="many-to-one", remote_ap="root")
        child_property = Property(
            tuple, relation="one-to-many", remote_ap="level2",
            remote_property="parent")
        level1_ap = Memory(
            {"id": Property(int), "label": Property(unicode),
             "parent": parent_property, "children": child_property}, ("id",))

        parent_property = Property(
            Item, relation="many-to-one", remote_ap="level1")
        level2_ap = Memory(
            {"id": Property(int), "label": Property(unicode),
             "parent": parent_property}, ("id",))

        self.site = Site()
        self.site.register("root", root_ap)
        self.site.register("level1", level1_ap)
        self.site.register("level2", level2_ap)

        rootitem = self.site.create("root", {"label": "root", "id": 0})
        rootitem.save()

        item1 = self.site.create(
            "level1", {"label": "1", "id": 1, "parent": rootitem})
        item1.save()
        item2 = self.site.create(
            "level1", {"label": "2", "id": 2, "parent": rootitem})
        item2.save()

        item11 = self.site.create(
            "level2", {"label": "1.1", "id": 1, "parent": item1})
        item11.save()
        item12 = self.site.create(
            "level2", {"label": "1.2", "id": 2, "parent": item1})
        item12.save()

        item21 = self.site.create(
            "level2", {"label": "2.1", "id": 3, "parent": item2})
        item21.save()
        item22 = self.site.create(
            "level2", {"label": "2.2", "id": 4, "parent": item2})
        item22.save()

        rootitem["children"] = [item1, item2]
        rootitem.save()

        item1["children"] = [item11, item12]
        item1.save()
        item2["children"] = [item21, item22]
        item2.save()
    # pylint: enable=C0103
