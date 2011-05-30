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

from nose.tools import nottest
from functools import update_wrapper, partial

from kalamar.site import Site


COMMON_TESTS = []


def require(module):
    """Decorator returning a function if the required ``module`` is present."""
    def test_if_available(module, function):
        """Test if ``module`` is available."""
        # With or without the exception, the function is returned
        # pylint: disable=W0150
        try:
            __import__(module)
        except ImportError:
            function.__test__ = False
            function.__available__ = False
        finally:
            return function
        # pylint: enable=W0150
    return partial(test_if_available, module)


def fill_site(site):
    """Fill a ``site`` with testing data."""
    site.create("things", {"id": 1, "name": "foo"}).save()
    site.create("things", {"id": 2, "name": "bar"}).save()
    site.create("things", {"id": 3, "name": "bar"}).save()


def make_site(access_point, fill=True):
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


def run_common(function):
    """``function`` decorator asking to call common tests for an access point.

    When called, ``function`` can return one, two or three values:

    - An access point
    - A test runner (``_default_runner`` used if not given)
    - A test title (access point type used if not given)

    If that function meets the nose requirements to be tested (for example,
    contains ``test`` in its name), this access point instance will be tested
    over all common tests.

    """
    # This function is magic, so highly commented.

    # First of all, try to find if calling function returns 1, 2 or 3 values.
    # 1st value (make_ap): function called to create the access point.
    # 2nd value (runner): function called to create the site and run the test.
    # 3rd value (title): string used as a prefix to the test title.
    runner, title = None, None

    # We try here to launch the function. Errors may happen, particularly
    # because of missing imports, and we should silently ignore them, as they
    # are already managed the ``require`` function.
    try:
        values = function()
    except ImportError:
        values = None

    if isinstance(values, tuple):
        # 2 or 3 values, the access point is the first value returned by
        # function(). We create a lambda function, called make_ap, calling
        # function() and returning the first value (the access point).
        make_ap = lambda: function()[0]

        # runner is the second value returned by function().
        runner = values[1]
        if len(values) == 3:
            # 3 values, title is the third one.
            title = values[2]
    else:
        # 1 value, function is make_ap.
        make_ap = function

    def test_run():
        """Create site and yield testing function for all common tests."""
        # This is a test generator, yielding a tuple containing a test runner
        # and a test.  For each common test, we generate a unique runner with a
        # unique description containing the access point type and the common
        # test description.  In other words, generated tests are the cartesian
        # product of common tests and tested access points.
        for test in COMMON_TESTS:
            # Try to get an access point, may be useless if a runner and a
            # title are given.
            access_point = make_ap() if make_ap else None

            # The runner can be the given runner (if 2+ values returned by
            # ``function``) of the default one (if 1 value is returned).  In
            # both cases, we must be sure that the function is unique in order
            # to give it a unique description.  To have a unique function, we
            # use a lambda function, even when runner is given.
            # pylint: disable=W0108
            if runner:
                # Lambda function calling given runner.
                _runner = lambda test: runner(test)
            else:
                # Default function calling test with a site created from
                # access_point.
                _runner = lambda test: test(
                    make_site(access_point, fill=not hasattr(test, "nofill")))
            # pylint: enable=W0108

            # The description is given if the test fails.  Having the access
            # point and the test description is very useful for debugging.
            _runner.description = "%s: %s" % (
                title or type(access_point).__name__, test.__doc__)

            # Finally yield the runner and the test, tranforming test_run into
            # a possible test generator.
            yield _runner, test

    # test_run will be registered to nose as possibly tested test generator,
    # thanks to the attributes of ``function`` (mainly its name and its place
    # in the test module).  We need to be sure that test_run looks like
    # ``function`` in order to be correctly detected.
    update_wrapper(test_run, function)

    # The run_common decorator returns test_run, that is a test generator.
    # Thus, in the access point test file, calling function with a run_common
    # decorator yields tests that are detected by nose.
    return test_run
