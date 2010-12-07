# Nose redefines assert_raises
# pylint: disable=E0611
from nose.tools import eq_
# pylint: enable=E0611

from .test_combinations import common

@common
def test_view_simple(site):
    """The simplest view request"""
    results = list(site.view('first_ap'))
    eq_(len(results), 5)
