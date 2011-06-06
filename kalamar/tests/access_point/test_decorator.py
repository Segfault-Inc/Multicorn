# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Aliases test.

Test the aliases backend.

"""

from kalamar.access_point.memory import Memory
from kalamar.access_point.decorator import Decorator, DecoratorProperty
from kalamar.property import Property

from ..common import run_common, make_site



class SimpleDecorator(Decorator):
    """Simple Decorator access point evaluating its values."""
    def preprocess_save(self, item):
        if len(item.unsaved_properties):
            for key in item.unsaved_properties:
                try:
                    values = eval(item.unsaved_properties[key])
                except:
                    values = item.unsaved_properties.getlist(key)
                item.setlist("base_%s" % key, values)


class SimpleDecoratorProperty(DecoratorProperty):
    """Item for the simple decorator access point."""
    def getter(self, item):
        return item.getlist("base_name")


def make_ap():
    """Create a simple access point."""
    underlying_access_point = Memory(
        {"id": Property(int), "base_name": Property(unicode)}, ("id",))
    decorated_prop = SimpleDecoratorProperty(unicode)
    return SimpleDecorator(underlying_access_point, {"name": decorated_prop})

@run_common
def test_alias():
    """Launch common tests for aliases."""
    return make_ap()

def runner(test):
    """Test runner for ``test``."""
    access_point = make_ap()
    site = make_site(access_point, fill=False)
    if not hasattr(test, "nofill"):
        site.create("things", {"id": 1,
            "name": "('foobar'.replace('bar', ''),)",
            "base_name": None}).save()
        site.create("things", {"id": 2,
            "name": "('foobar'.replace('foo', ''),)",
            "base_name": None}).save()
        site.create("things", {"id": 3,
            "name": "('foobar'.replace('foo', ''),)",
            "base_name": None}).save()
    test(site)

@run_common
def test_decorator_common():
    """Define a custom test runner for the common tests."""
    return None, runner, "Decorator"
