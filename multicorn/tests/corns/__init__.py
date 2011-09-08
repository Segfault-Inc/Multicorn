# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from attest import Tests
from functools import wraps
from . import base, tests
from multicorn import Multicorn
from multicorn.utils import colorize


def module_name_docstring(fun, module_name):
    @wraps(fun)
    def wrapper(corn, data):
        return fun(corn, data)
    wrapper.__doc__ = "[%s]%s" % (
        colorize('yellow', module_name),
        colorize('green', fun.__doc__ or "Untitled test"))
    return wrapper


def make_test_suite(make_corn, module_name, data=None, teardown=None):
    if data is None:
        data_fun = lambda: tuple(
            [{'id': 1, 'name': u'foo', 'lastname': u'bar'},
             {'id': 2, 'name': u'baz', 'lastname': u'bar'},
             {'id': 3, 'name': u'foo', 'lastname': u'baz'}])
    else:
        data_fun = data

    emptysuite = Tests()
    fullsuite = Tests()

    @emptysuite.context
    def emptycontext():
        mc = Multicorn()
        corn = make_corn()
        mc.register(corn)
        try:
            yield corn, data_fun()

        finally:
            if teardown:
                teardown(corn)

    @fullsuite.context
    def fullcontext():
        mc = Multicorn()
        corn = make_corn()
        mc.register(corn)
        try:
            for item in data_fun():
                corn.create(item).save()
            yield corn, data_fun()

        finally:
            if teardown:
                teardown(corn)

    for test_prototype in base.EMPTYTESTS:
        emptysuite.test(module_name_docstring(test_prototype, module_name))

    for test_prototype in base.FULLTESTS:
        fullsuite.test(module_name_docstring(test_prototype, module_name))

    if data is None:
        for test_prototype in tests.TESTS:
            fullsuite.test(
                module_name_docstring(test_prototype, module_name))

    return emptysuite, fullsuite
