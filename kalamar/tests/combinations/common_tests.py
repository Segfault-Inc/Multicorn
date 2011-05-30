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
Common tests for access point combinations.

"""

# Nose redefines assert_raises
# pylint: disable=E0611
from nose.tools import eq_
# pylint: enable=E0611
from kalamar.request import Condition, And, Or, make_request
from kalamar.query import QueryFilter
from kalamar import func

from .test_combinations import common


@common
def test_view_simple(site):
    """The simplest view request."""
    results = list(site.view("first_ap"))
    eq_(len(results), 5)

@common
def test_view_filters(site):
    """Various view filters."""
    condition = Condition("id", ">", 3)
    results = list(site.view("first_ap", request=condition))
    eq_(len(results), 2)
    condition = Condition("color", "!=", "blue")
    results = list(site.view("first_ap", request=condition))
    eq_(len(results), 3)
    condition = Condition("second_ap.code", "=", "BBB")
    results = list(site.view("first_ap", request=condition))
    eq_(len(results), 2)
    condition = And(
            Condition("color", "!=", "blue"),
            Condition("second_ap.code", "=", "BBB"))
    results = list(site.view("first_ap", request=condition))
    eq_(len(results), 1)
    condition = Or(
            Condition("color", "!=", "blue"),
            Condition("second_ap.code", "=", "BBB"))
    results = list(site.view("first_ap", request=condition))
    eq_(len(results), 4)
    condition = And(
            Condition("id", ">", 3),
            Or(Condition("color", "!=", "blue"),
               Condition("second_ap.code", "=", "BBB")))
    results = list(site.view("first_ap", request=condition))
    eq_(len(results), 2)

@common
def test_order(site):
    """Asserts that an order clause is working."""
    order = [("color", True), ("name", False)]
    results = list(site.view("first_ap", order_by=order))
    assert(all([a["color"] < b["color"] or
                (a["color"] == b["color"] and a["name"] >= b["name"])
                for a, b in  zip(results[:-1], results[1:])]))

@common
def test_mapping(site):
    """Test various selections mappings."""
    mapping = {"second_ap_name": "second_ap.name"}
    results = list(site.view("first_ap", aliases=mapping ))
    eq_(len(results), 5)
    assert(all([a["second_ap_name"] in [None, "second_ap AAA", "second_ap BBB"]
                for a in results]))
    mapping = {"id": "id", "label": "name"}
    results = list(site.view("first_ap", aliases=mapping))
    eq_(len(results), 5)
    assert all(["id" in a and "label" in a for a in results])

@common
def test_distinct(site):
    """Test a ``distinct`` view query."""
    results = list(site.view("first_ap", {"color": "color"}, distinct=True))
    eq_(len(results), 3)

@common
def test_range(site):
    """Test the range Query."""
    results = list(site.view("first_ap", select_range=(1, 2)))
    eq_(len(results), 1)
    results = list(site.view("first_ap", {"color": "color"}, distinct=True,
                             order_by=[("color", True)], select_range=(1, 2)))
    eq_(len(results), 1)
    eq_(results[0]["color"], "green")

@common
def test_one_to_many(site):
    """Test one to many relationships traversals."""
    mapping = {"fname": "first_aps.name"}
    results = list(site.view("second_ap", aliases=mapping))
    eq_(len(results), 4)
    request = {"first_aps.color": "blue"}
    results = list(site.view("second_ap", aliases=mapping, request=request))
    eq_(len(results), 2)

@common
def test_transform(site):
    """Test that simple transform function work"""
    mapping = {"fname": func.slice("first_aps.name", [2,4])}
    results = list(site.view("second_ap", aliases=mapping))
    eq_(len(results), 4)
    assert all((len(res['fname']) == 2 for res in results))
    results = list(site.view("first_ap",
        request={"id": 4},
        aliases={"second_ap_name": func.coalesce("second_ap.name", "(Empty)")}))
    eq_(len(results), 1)
    eq_(results[0]['second_ap_name'], '(Empty)')



@common
def test_aggregate(site):
    """Test simple aggregation functions"""
    agg = {"count": func.count()}
    results = list(site.view("second_ap", aggregate=agg))
    eq_(len(results), 1)
    eq_(results[0]['count'], 2)
    agg = {"count": func.count(), "second_ap_name": ""}
    results = site.view("first_ap",
        aliases={"second_ap_name": "second_ap.name"},
        aggregate=agg)
    counts = dict((result["second_ap_name"], result["count"]) for result in results)
    eq_({None: 1, 'second_ap AAA': 2, 'second_ap BBB': 2}, counts)


@common
def test_many_to_one(site):
    """Test many to one relationship traversals."""
    request = {"second_ap.code": "BBB"}
    results = list(
        site.view("first_ap", {"scode": "second_ap.code"}, request=request))
    eq_(len(results), 2)
    assert all((result["scode"] == "BBB" for result in results))
    query = QueryFilter(make_request(request))
    results = list(site.view("first_ap", query=query))
    eq_(len(results), 2)

@common
def test_item_condition(site):
    """Test a condition on an item directly."""
    item = site.open("second_ap", {"code": "AAA"})
    condition = Condition("second_ap", "=", item)
    items = list(site.search("first_ap", condition))
    eq_(len(items), 2)
    items = list(site.view("first_ap", request=condition))
    eq_(len(items), 2)

@common
def test_item_loading(site):
    """Test that many to one and one to many loaders work."""
    item = site.open("second_ap", {"code": "AAA"})
    first_items = item["first_aps"]
    eq_(len(first_items), 2)
    assert all([first["second_ap"] == item
        for first in first_items])
