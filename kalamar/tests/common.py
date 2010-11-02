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
from kalamar import Site
from functools import update_wrapper


COMMON_TESTS = []


def fill_site(site):
    """Fill a ``site`` with testing data."""
    site.create("things", {"id": 1, "name": u"foo"}).save()
    site.create("things", {"id": 2, "name": u"bar"}).save()
    site.create("things", {"id": 3, "name": u"bar"}).save()


def make_site(access_point, fill=False):
    """Create a site from an ``access_point`` filled by ``fill`` values."""
    site = Site()
    site.register("things", access_point)

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


def run_common(make_ap):
    """Decorator over a function that return an instance of access point.

    If that function meet the noses requirements to be tested, this
    access_point instance will be tested over all common tests.

    """
    def test_run():
        """Create site and yield testing function for all common tests."""
        for test in COMMON_TESTS:
            run_test = lambda test: make_site(
                make_ap(), fill=not hasattr(test, "nofill"))
            yield run_test, test

    update_wrapper(test_run, make_ap)
    return test_run
