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

def make_site(ap, fill=False):
    '''
    Create a site from an instance of access point *ap*. *fill* it with tests
    values.
    '''

    site = Site()
    site.register("things", ap)

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

    def test_run():
        for test in commontest.tests:
            site = make_site(make_ap(), fill=not hasattr(test, 'nofill'))
            yield test, site

    update_wrapper(test_run, make_ap)

    return test_run

# add here every modules that contains common test
# this code is at the end of the module to avoid cyclic import issue
import kalamar.tests.test_common
