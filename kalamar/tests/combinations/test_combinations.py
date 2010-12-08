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
# along with Kraken.  If not, see <http://www.gnu.org/licenses/>.

"""
Common test architecture.

Functions in this module allow to run a set of common tests against every
access point.

"""

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
    second_ap_item1 = site.create("second_ap", {"code" : "AAA", "name":
    "second_ap AAA"})
    second_ap_item1.save()
    second_ap_item2 = site.create("second_ap", {"code" : "BBB", "name":
    "second_ap BBB"})
    second_ap_item2.save()
    item = site.create("first_ap", {'id': 1, 'name': 'Test one', 'color':
        'red', 'second_ap' : second_ap_item1})
    item.save()
    item = site.create("first_ap", {'id': 2, 'name': 'Test two', 'color':
        'red', 'second_ap': second_ap_item2})
    item.save()
    item = site.create("first_ap", {'id': 3, 'name': 'Test three',
        'color': 'blue', 'second_ap': second_ap_item1})
    item.save()
    item = site.create("first_ap", {'id': 4, 'name': 'Test four',
        'color': 'green', 'second_ap': None})
    item.save()
    item = site.create("first_ap", {'id': 5, 'name': 'Test five',
        'color': 'blue', 'second_ap': second_ap_item2})
    item.save()




def make_site(first_ap, second_ap, fill=True):
    """Create a site from an ``access_point`` filled by ``fill`` values."""
    site = Site()
    site.register("first_ap", first_ap)
    site.register("second_ap", second_ap)

    if fill:
        fill_site(site)
    return site



def nofill(function):
    """Decorator saying that ``function`` needs an empty (unfilled) site."""
    function.nofill = False
    return function


def common(function):
    """Decorator to explicit a test which must run for all access points.

    All the tests are saved in :const:`COMMON_TESTS`.

    """
    function = nottest(function)
    COMMON_TESTS.append(function)
    return function

def first_ap(function):
    function = nottest(function)
    FIRST_APS.append(function)
    return function

class ap_decorator(object):
    """ Decorator class to collect different access points """

    def __init__(self, setup=None, teardown=None):
        self.setup = setup
        self.teardown = teardown

    def __call__(self, function):
        function = nottest(function)
        function.setup = self.setup
        function.teardown = self.teardown
        self.aps.append(function)
        return function


class first_ap(ap_decorator):
    """Decorator to mark a function as yielding an access point to be tested.

    The first access point must have the following properties :
        - id: int, identity_property
        - name: unicode
        - color: unicode
        - second_ap : Item, remote_ap="second_ap",

    """

    aps = FIRST_APS

class second_ap(ap_decorator):
    """Decorator to mark a function as yielding an access point to be tested.

    The second access point must have the following properties :
        - code: unicode, identity_property
        - name: unicode
        - first_aps: iter, relation=one_to_many, remote_ap=first_ap,
          remote_property=second_ap

    """
    aps = SECOND_APS


class first_wrapper(ap_decorator):
    """Decorator to mark a function as a ``double cheese generator``.

    It must accept a function returning an access point conforming to the first
    access point definition, and returns a function returning a wrapped access
    point.
    """
    aps = FIRST_WRAPPERS

class second_wrapper(ap_decorator):
    aps = SECOND_WRAPPERS



from .access_point import *
from .common_tests import *


def test_combinations():

    def make_wrapped_setup(func):
        return lambda ap : func(ap.wrapped_ap)
    for wrapper, first_ap_func in list(product(FIRST_WRAPPERS, FIRST_APS)):
        kwargs = {}
        if first_ap_func.setup:
            kwargs['setup'] = make_wrapped_setup(first_ap_func.setup)
        if first_ap_func.teardown:
            kwargs['teardown'] = make_wrapped_setup(first_ap_func.teardown)
        first_ap(**kwargs)(wrapper(first_ap_func))
    for wrapper, second_ap_func in list(product(SECOND_WRAPPERS, SECOND_APS)):
        kwargs = {}
        if second_ap_func.setup:
            kwargs['setup'] = make_wrapped_setup(second_ap_func.setup)
        if second_ap_func.teardown:
            kwargs['teardown'] = make_wrapped_setup(second_ap_func.teardown)
        second_ap(**kwargs)(wrapper(second_ap_func))

    def make_closure(func, ap):
        return lambda : func(ap)
    for first_ap_func, second_ap_func in product(FIRST_APS, SECOND_APS):
        for test in COMMON_TESTS:
            first_ap_instance = first_ap_func()
            second_ap_instance = second_ap_func()
            ordered_ap_dict = zip((first_ap_func, second_ap_func), (first_ap_instance, second_ap_instance))
            _runner = lambda test: test(make_site(first_ap_instance,
                second_ap_instance,
                fill=not hasattr(test, "nofill")))
            _runner.description = "#1: %s, #2: %s. Test: %s" % (
                type(first_ap_instance).__name__,
                type(second_ap_instance).__name__,
                test.__doc__)
            setups = []
            teardowns = []
            for func, ap in ordered_ap_dict:
                if func.setup is not None:
                    setups.append(make_closure(func.setup, ap))
                if func.teardown is not None:
                    teardowns.append(make_closure(func.teardown, ap))
            _runner.setup = lambda: [setup() for setup in setups]
            _runner.tearDown = lambda: [teardown() for teardown in teardowns]
            yield _runner, test
