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
Common test architecture.

Functions in this module allow to run a set of common tests against every
access point.

"""

import shutil
from nose.tools import nottest
from itertools import product
from kalamar.site import Site


COMMON_TESTS = []
FIRST_APS = []
SECOND_APS = []
FIRST_WRAPPERS = []
SECOND_WRAPPERS = []


def fill_site(site):
    """Fill a ``site`` with testing data."""
    second_ap_item1 = site.create(
        "second_ap", {"code": "AAA", "name": "second_ap AAA"})
    second_ap_item1.save()
    second_ap_item2 = site.create(
        "second_ap", {"code": "BBB", "name": "second_ap BBB"})
    second_ap_item2.save()
    item = site.create("first_ap", {"id": 1, "name": "Test one",
        "color": "red", "second_ap": second_ap_item1})
    item.save()
    item = site.create("first_ap", {"id": 2, "name": "Test two",
        "color": "red", "second_ap": second_ap_item2})
    item.save()
    item = site.create("first_ap", {"id": 3, "name": "Test three",
        "color": "blue", "second_ap": second_ap_item1})
    item.save()
    item = site.create("first_ap", {"id": 4, "name": "Test four",
        "color": "green", "second_ap": None})
    item.save()
    item = site.create("first_ap", {"id": 5, "name": "Test five",
        "color": "blue", "second_ap": second_ap_item2})
    item.save()

def make_site(first_ap, second_ap, fill=True):
    """Create a site from an ``access_point`` filled by ``fill`` values."""
    site = Site()
    site.register("first_ap", first_ap)
    site.register("second_ap", second_ap)

    if fill:
        fill_site(site)
    return site

def common(function):
    """Decorator to explicit a test which must run for all access points.

    All the tests are saved in :const:`COMMON_TESTS`.

    """
    function = nottest(function)
    COMMON_TESTS.append(function)
    return function

def teardown_fs(access_point):
    """Remove the root filesystem directory of the wrapped ``access_point``."""
    shutil.rmtree(access_point.wrapped_ap.root_dir)


class APDecorator(object):
    """Decorator class to collect different access points."""
    aps = None

    def __init__(self, teardown=None):
        self.teardown = teardown

    def __call__(self, function):
        function = nottest(function)
        function.teardown = self.teardown
        if getattr(function, "__available__", True):
            self.aps.append(function)
        return function


class FirstAP(APDecorator):
    """Decorator to mark a function as yielding an access point to be tested.

    The first access point must have the following properties:

    - id: int, identity_property
    - name: unicode
    - color: unicode
    - second_ap: Item, remote_ap="second_ap",

    """
    aps = FIRST_APS


class SecondAP(APDecorator):
    """Decorator to mark a function as yielding an access point to be tested.

    The second access point must have the following properties:

    - code: unicode, identity_property
    - name: unicode
    - first_aps: iter, relation="one_to_many", remote_ap="first_ap",
          remote_property="second_ap"

    """
    aps = SECOND_APS


class FirstWrapper(APDecorator):
    """Decorator to mark a function as a first ``double cheese generator``.

    It must accept a function returning an access point conforming to the first
    access point definition, and returns a function returning a wrapped access
    point.

    """
    aps = FIRST_WRAPPERS


class SecondWrapper(APDecorator):
    """Decorator to mark a function as a second ``double cheese generator``."""
    aps = SECOND_WRAPPERS


# Importing all access points and common tests are imported here with a
# wildcard, this is very useful and very efficient
# pylint: disable=W0401
# pylint: disable=W0614
from .access_point import *
from .common_tests import *
# pylint: enable=W0401
# pylint: enable=W0614


def test_combinations():
    """Test the access point combinations."""
    def make_wrapped_setup(function):
        """Create a wrapped setup for given ``func``."""
        return lambda access_point: function(access_point.wrapped_ap)
    for wrapper, first_ap_func in list(product(FIRST_WRAPPERS, FIRST_APS)):
        kwargs = {}
        if first_ap_func.teardown:
            kwargs["teardown"] = make_wrapped_setup(first_ap_func.teardown)
        FirstAP(**kwargs)(wrapper(first_ap_func))
    for wrapper, second_ap_func in list(product(SECOND_WRAPPERS, SECOND_APS)):
        kwargs = {}
        if second_ap_func.teardown:
            kwargs["teardown"] = make_wrapped_setup(second_ap_func.teardown)
        SecondAP(**kwargs)(wrapper(second_ap_func))

    def make_closure(function, access_point):
        """Create a closure of ``function(access_point)``."""
        return lambda: function(access_point)

    for first_ap_func, second_ap_func in product(FIRST_APS, SECOND_APS):
        for test in COMMON_TESTS:
            first_ap_instance = first_ap_func()
            second_ap_instance = second_ap_func()
            ordered_ap_dict = zip(
                (first_ap_func, second_ap_func),
                (first_ap_instance, second_ap_instance))
            _runner = lambda test: test(
                make_site(first_ap_instance, second_ap_instance,
                          fill=not hasattr(test, "nofill")))
            _runner.description = "#1: %s, #2: %s. Test: %s" % (
                type(first_ap_instance).__name__,
                type(second_ap_instance).__name__,
                test.__doc__)
            teardowns = []
            for function, access_point in ordered_ap_dict:
                if function.teardown is not None:
                    teardowns.append(
                        make_closure(function.teardown, access_point))
            _runner.tearDown = lambda: [teardown() for teardown in teardowns]
            yield _runner, test
