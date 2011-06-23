# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests
from functools import wraps
from . import tests
from multicorn import Multicorn
from multicorn.utils import colorize


def module_name_docstring(fun, module_name):
    @wraps(fun)
    def wrapper(arg):
        return fun(arg)
    wrapper.__doc__ = "[%s]%s" % (
        colorize('yellow', module_name),
        colorize('green', fun.__doc__ or "Untitled test"))
    return wrapper


def make_test_suite(make_corn, module_name, teardown=None):
    suite = Tests()

    @suite.context
    def context():
        mc = Multicorn()
        corn = make_corn()
        mc.register(corn)
        try:
            yield corn
        finally:
            if teardown:
                teardown(corn)

    for test_prototype in tests.TESTS:
        suite.test(module_name_docstring(test_prototype, module_name))
    return suite
