"""Common test architecture, allowing to run a set of common tests against every
access point
"""
from nose.tools import nottest
from kalamar import Site
from functools import update_wrapper


# see at the end of the module for import of all common test files

# utils functions
def fill_site(site):
    '''
    File a *site* with tests data.
    '''
    site.create("things", {"id": 1, "name": u"foo"}).save()
    site.create("things", {"id": 2, "name": u"bar"}).save()
    site.create("things", {"id": 3, "name": u"bar"}).save()

def make_site(access_point, fill=False):
    '''
    Create a site from an instance of access point *access_point*. *fill* it with tests
    values.
    '''

    site = Site()
    site.register("things", access_point)

    if fill:
        fill_site(site)

    return site

def nofill(f):
    '''
    Decorator saying that f test function needs an empty (unfilled) site

    '''
    f.nofill = False
    return f

def commontest(f):
    '''
    Decorator to explicit a test which must run for all access points. All the
    tests are saved in commontest.tests.
    '''
    f = nottest(f)
    commontest.tests.append(f)
    return f

commontest.tests = []

def run_common(make_ap):
    '''
    Decorator over a function that return an instance of access point.

    If that function meet the noses requirements to be tested, this
    access_point instance will be tested over all common tests.
    '''
    def _run_test(test):
        test(make_site(make_ap(), fill=not hasattr(test, 'nofill')))
        
    def test_run():
        for test in commontest.tests:
            yield _run_test, test

    update_wrapper(test_run, make_ap)

    return test_run

# add here every modules that contains common test
# this code is at the end of the module to avoid cyclic import issue
import kalamar.tests.test_common
