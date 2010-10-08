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
View test
=========

Test the view request algorithm.

"""

from nose.tools import eq_, nottest
from kalamar.request import Condition, Or, Request
from kalamar.access_point.memory import Memory
from kalamar.property import Property
from kalamar.site import Site
from kalamar.query import BadQueryException


@nottest
def make_test_site():
    child_property = Property(
        iter, relation="one-to-many", remote_ap="level1",
        remote_property="parent")
    root_ap = Memory(
        {"id": Property(int), "label": Property(unicode),
         "children": child_property}, "id")

    parent_property = Property(iter, relation="many-to-one", remote_ap="root")
    child_property = Property(
        iter, relation="one-to-many", remote_ap="level2",
        remote_property="parent")
    level1_ap = Memory(
        {"id": Property(int), "label": Property(unicode),
         "parent": parent_property, "children": child_property}, "id")

    parent_property = Property(iter, relation="many-to-one", remote_ap="level1")
    level2_ap = Memory(
        {"id": Property(int), "label": Property(unicode),
         "parent": parent_property}, "id")

    site = Site()
    site.register("root", root_ap)
    site.register("level1", level1_ap)
    site.register("level2", level2_ap)
    return site

@nottest
def init_data():
    site = make_test_site()
    rootitem = site.create("root", {"label": "root", "id": 0})
    rootitem.save()

    item1 = site.create("level1", {"label": "1", "id": 1, "parent": rootitem})
    item1.save()
    item2 = site.create("level1", {"label": "2", "id": 2, "parent": rootitem})
    item2.save()

    item11 = site.create("level2", {"label": "1.1", "id": 1, "parent": item1})
    item11.save()
    item12 = site.create("level2", {"label": "1.2", "id": 2, "parent": item1})
    item12.save()

    item21 = site.create("level2", {"label": "2.1", "id": 3, "parent": item2})
    item21.save()
    item22 = site.create("level2", {"label": "2.2", "id": 4, "parent": item2})
    item22.save()

    rootitem["children"] = [item1, item2]
    rootitem.save()

    item1["children"] = [item11, item12]
    item1.save()
    item2["children"] = [item21, item22]
    item2.save()

    return site

def test_lazy():
    site = init_data()
    root = site.open("root", {"id": 0})
    eq_(len(list(root["children"])), 2)

def test_bad_request():
    site = init_data()
    try:
        list(site.view("root", {"leaf_label": "children.grou"}, {}))
    except BadQueryException:
        pass
    else:
        assert False, "Expected KeyError."

    try:
        list(site.view("root", {"leaf_label": "children.label"},
            {"children.grou" : 4}))
    except BadQueryException:
        pass
    else:
        assert False, "Expected KeyError."


    try:
        list(site.view("root", {"leaf_label": "children.label"},
            {"children.children.id" : "abc"}))
    except BadQueryException:
        pass
    else:
        assert False, "Expected ValueError."

    


    

def test_first_level():
    site = init_data()
    aliases = {"leaf_label": "children.label", "root_label": "label"}
    items = list(site.view("root", aliases, {}))
    eq_(len(items), 2)

def test_star_request():
    site = init_data()
    aliases = {'': '*', 'children_' : 'children.*'}
    items = list(site.view("root", aliases, {}))
    eq_(len(items), 2)
    item = items[0]
    assert(all([alias in item for alias in ["label", "id", "children_label",
    "children_id"]]))
    
def test_star_request_with_cond():
    site = init_data()
    aliases = {'': '*', 'children_' : 'children.*'}
    request = {'label': 'root', 'children_id' : 1}
    items = list(site.view("root", aliases, request))
    eq_(len(items), 1)
    item = items[0]
    assert(all([alias in item for alias in ["label", "id", "children_label",
    "children_id"]]))


def test_first_level_with_cond():
    site = init_data()
    aliases = {"leaf_label": "children.label", "root_label": "label"}
    conditions = {"children.label": "1"}
    items = list(site.view("root", aliases, conditions))
    eq_(len(items), 1)


def test_leaf_nodes():
    site = init_data()
    aliases = {
        "leaf_label": "children.children.label", "root_label": "label",
        "middle_label":"children.label"}
    condition = Condition("children.children.label", "=", "2.2")
    items = list(site.view("root", aliases, condition))
    eq_(len(items), 1)
    assert(all([item["leaf_label"] == "2.2" for item in items]))
    condition = Condition("children.label", "=", "2")
    items = list(site.view("root", aliases, condition))
    eq_(len(items), 2)
    assert(all([item["middle_label"] == "2" for item in items]))
    condition = {"children.label": "2", "children.children.label": "1.1"}
    items = list(site.view("root", aliases, condition))
    eq_(len(items), 0)
    
def test_complex_condition():
    site = init_data()
    aliases = {"root_label": "label", "middle_label": "children.label"}
    condition = Or(Condition("children.label", "=", "1"),
                   Condition("children.children.label", "=", "2.1"))
    items = list(site.view("root", aliases,condition))
    eq_(len(items), 3)

def test_many_to_ones():
    site = init_data()
