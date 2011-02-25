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
Common tests.

Common tests run against all access points.

"""

# Nose redefines assert_raises
# pylint: disable=E0611
from nose.tools import eq_, raises, assert_raises
# pylint: enable=E0611

from kalamar.access_point import MultipleMatchingItems, ItemDoesNotExist
from kalamar.item import MultiDict
from kalamar.request import Condition, Or
from kalamar.value import to_unicode
from kalamar.access_point.alchemy import Alchemy
from kalamar.access_point.mongo import Mongo
from kalamar.access_point.xml import XML
from kalamar.access_point.unicode_stream import UnicodeStream
from .common import nofill, common


# TODO: support of multiple values for access points should be introspectable
SINGLE_VALUE_ACCESS_POINTS = (Alchemy, UnicodeStream, XML, Mongo)


@nofill
@common
def test_single_item(site):
    """Save a single item and retrieve it."""
    site.create("things", {"id": 1, "name": "foo"}).save()
    all_items = list(site.search("things"))
    eq_(len(all_items), 1)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "foo")

@nofill
@common
def test_single_item_multidict(site):
    """Save a single item with multiple values and retrieve it."""
    properties = MultiDict()
    properties["id"] = 1
    properties.setlist("name", ("foo", "bar"))
    site.create("things", properties).save()
    all_items = list(site.search("things"))
    eq_(len(all_items), 1)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], "foo")
    if not isinstance(site.access_points["things"], SINGLE_VALUE_ACCESS_POINTS):
        eq_(item.getlist("name"), ("foo", "bar"))

@nofill
@common
def test_create_unicode(site):
    """Save a single item with unicode values and retrieve it."""
    name = to_unicode("Touché")
    properties = {"id": 1, "name": name}
    site.create("things", properties).save()
    all_items = list(site.search("things"))
    eq_(len(all_items), 1)
    item = all_items[0]
    eq_(item["id"], 1)
    eq_(item["name"], name)

@nofill
@common
@raises(ValueError)
def test_missing_properties(site):
    """Missing properties when creating an item raises an error."""
    site.create("things")

@nofill
@common
@raises(ValueError)
def test_double_properties(site):
    """Item with given and lazy properties raises an error."""
    site.create("things", {"id": 1, "name": "bar"}, {"name": lambda: None})

@nofill
@common
@raises(ValueError)
def test_unexpected_given_property(site):
    """Item with unexpected given property raises an error."""
    site.create("things", {"id": 1, "spam": "bar"}, {"name": lambda: None})

@nofill
@common
@raises(ValueError)
def test_unexpected_lazy_property(site):
    """Item with unexpected lazy property raises an error."""
    site.create("things", {"id": 1, "name": "bar"}, {"spam": lambda: None})

@common
def test_search(site):
    """Test a simple search."""
    results = site.search("things", {"name": "bar"})
    eq_(set(item["id"] for item in results), set([2, 3]))

@common
def test_complex_search(site):
    """Test a complex search."""
    condition = Or(Condition("name", "=", "bar"), Condition("id", "<", 2))
    results = site.search("things", condition)
    eq_(set(item["id"] for item in results), set([1, 2, 3]))

@common
def test_re_equal_search(site):
    """Test a search with a re equality."""
    condition = Condition("name", "~=", ".?a.*")
    results = site.search("things", condition)
    eq_(set(item["id"] for item in results), set([2, 3]))

@common
def test_re_inequal_search(site):
    """Test a search with a re inequality."""
    condition = Condition("name", "~!=", ".?a.*")
    results = site.search("things", condition)
    eq_(set(item["id"] for item in results), set([1]))

@common
def test_inequal_search(site):
    """Test a search with an inequality condition."""
    condition = Condition("name", "!=", "bar")
    results = site.search("things", condition)
    eq_(set(item["id"] for item in results), set([1]))

@common
def test_like_search(site):
    """Test a search with an inequality condition."""
    condition = Condition("name", "like", "b%r")
    results = site.search("things", condition)
    eq_(set(item["id"] for item in results), set([2, 3]))

@common
def test_open_one(site):
    """Standard ``open``."""
    result = site.open("things", {"name": "foo"})
    eq_(result["id"], 1)

@common
def test_modify_identity(site):
    """Test the identity modification of an item."""
    item = site.open("things", {"name": "foo"})
    assert_raises(KeyError, item.__setitem__, "id", 500)
    item = site.create("things", {"name": "bar", "id": 400})
    item["id"] = 500
    item.save()
    assert_raises(KeyError, item.__setitem__, "id", 500)

@common
def test_update_identity(site):
    """Test the identity modification of an item."""
    item = site.open("things", {"name": "foo"})
    assert_raises(KeyError, item.update, (("id", 500),))
    item = site.create("things", {"name": "bar", "id": 400})
    item.update(id=500)
    item.save()
    assert_raises(KeyError, item.update, ("id", 500))

@common
@raises(MultipleMatchingItems)
def test_open_two(site):
    """Exception raised when muliple items match ``open``."""
    site.open("things", {"name": "bar"})

@common
@raises(ItemDoesNotExist)
def test_open_zero(site):
    """Exception raised when no item match ``open``."""
    site.open("things", {"name": "nonexistent"})

@common
def test_open_one_default(site):
    """Standard ``open`` with ``default``."""
    result = site.open("things", {"name": "foo"}, "spam")
    eq_(result["id"], 1)

@common
@raises(MultipleMatchingItems)
def test_open_two_default(site):
    """Exception raised when muliple items match ``open`` with ``default``."""
    site.open("things", {"name": "bar"}, "spam")

@common
def test_open_zero_default(site):
    """Default returned when no item match ``open`` with ``default``."""
    eq_(site.open("things", {"name": "nonexistent"}, "spam"), "spam")

@common
def test_modify(site):
    """Standard edition of an item."""
    item = site.open("things", {"name": "foo"})
    identifier = item["id"]
    item["name"] = "spam"
    item.save()
    item = site.open("things", {"name": "spam"})
    eq_(item["id"], identifier)

@common
def test_update(site):
    """Standard update of an item."""
    item = site.open("things", {"name": "foo"})
    identifier = item["id"]
    item.update({"name": "spam"})
    item.save()
    item = site.open("things", {"name": "spam"})
    eq_(item["id"], identifier)
    item.update(name="egg")
    item.save()
    item = site.open("things", {"name": "egg"})
    eq_(item["id"], identifier)
    item.update((("name", "spam"),))
    item.save()
    item = site.open("things", {"name": "spam"})
    eq_(item["id"], identifier)
    assert_raises(ValueError, item.update, ("name", 500))
    assert_raises(TypeError, item.update, {"name": 500}, {"name": 600})

@common
@raises(TypeError)
def test_delete_key(site):
    """Assert that deleting an item key raises an ``TypeError``."""
    item = site.open("things", {"name": "foo"})
    del item["name"]

@common
def test_modify_list(site):
    """Edition of an item with a list of values."""
    item = site.open("things", {"name": "foo"})
    identifier = item["id"]
    item.setlist("name", ("spam", "egg"))
    item.save()
    item = site.open("things", {"name": "spam"})
    eq_(item["id"], identifier)
    eq_(item["name"], "spam")
    if not isinstance(site.access_points["things"], SINGLE_VALUE_ACCESS_POINTS):
        eq_(item.getlist("name"), ("spam", "egg"))

@common
def test_delete(site):
    """Test a simple delete."""
    item = site.open("things", {"name": "foo"})
    item.delete()
    eq_(list(site.search("things", {"name": "foo"})), [])

@common
def test_delete_many(site):
    """Test a multiple delete."""
    site.delete_many("things", {"name": "bar"})
    eq_(list(site.search("things", {"name": "bar"})), [])

@common
def test_delete_many_id(site):
    """Test a multiple delete using a id-based request."""
    site.delete_many("things", {"id": 1})
    eq_(list(site.search("things", {"id": 1})), [])

@common
def test_delete_many_complex(site):
    """Test a multiple delete using complex request."""
    condition = Or(Condition("id", "=", 1), Condition("id", "=", 2))
    site.delete_many("things", condition)
    eq_(list(site.search("things", condition)), [])

@common
def test_eq(site):
    """Test equality between items."""
    item1 = site.open("things", {"name": "foo"})
    item2 = site.open("things", {"name": "foo"})
    eq_(item1, item2)

@common
def test_view(site):
    """Test simple view request."""
    items = list(site.view("things", {"foo": "name"}))
    eq_(len(items), 3)
    assert(all("foo" in item for item in items))

@common
def test_view_condition(site):
    """Test simple view condition."""
    items = list(site.view("things", {"name":"name"}, {"name": "bar"}))
    eq_(len(items), 2)
    assert(all(item["name"] == "bar" for item in items))
    items = list(site.view("things", {"foo":"name"}, {"name": "bar"}))
    eq_(len(items), 2)
    assert(all(item["foo"] == "bar" for item in items))
    items = list(site.view("things", {"foo":"name"}, {"foo": "bar"}))
    eq_(len(items), 2)
    assert(all(item["foo"] == "bar" for item in items))

@common
def test_view_order_by(site):
    """Test order by argument."""
    items = list(site.view(
            "things", {"name": "name"}, {}, [("name", True)]))
    assert(all(n_1["name"] >= n["name"] for n, n_1 in zip(items, items[1:])))
    items = list(site.view(
            "things", {"name": "name", "id": "id"}, {},
            [("name", True), ("id", False)]))
    assert(all(
            n_1["name"] >= n["name"] and n_1["id"] <= n["id"]
            for n, n_1 in zip(items, items[1:])))

@common
def test_view_range(site):
    """Test range argument."""
    items = list(site.view("things", select_range=2))
    eq_(len(items), 2)
    items = list(site.view("things", order_by=[("id", True)],
        select_range=(1, 2)))
    eq_(len(items), 1)
    eq_(items[0]["id"], 2)

@common
def test_view_operators(site):
    """Test various operators with a view"""
    condition = Condition("foo", "!=", "bar")
    results = list(
        site.view("things", {"bar": "id" , "foo": "name"}, condition))
    eq_(set(item["bar"] for item in results), set([1]))
    condition = Condition("foo", "like", "b%r")
    results = list(
        site.view("things", {"bar": "id" , "foo": "name"}, condition))
    eq_(set(item["bar"] for item in results), set([2, 3]))

@common
def test_view_distinct(site):
    """Test the distinct view query"""
    results = list(site.view("things", {"foo": "name"}, distinct=True))
    eq_(len(results), 2)
    eq_(set(item["foo"] for item in results), set(["foo", "bar"]))

@common
def test_view_star_request(site):
    """Test a view with a wildcard."""
    items = list(site.view("things", {"": "*"}))
    eq_(len(items), 3)
    for item in items:
        assert(all(attr in item for attr in ("name", "id")))
    items = list(site.view("things", {"prefix_": "*"}))
    eq_(len(items), 3)
    for item in items:
        assert(all("prefix_%s" % attr in item for attr in ("name", "id")))
