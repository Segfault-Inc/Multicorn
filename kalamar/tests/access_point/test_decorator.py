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
Aliases test.

Test the aliases backend.

"""

# Nose redefines assert_raises
# pylint: disable=E0611
# pylint: enable=E0611

from kalamar.access_point.memory import Memory
from kalamar.access_point.decorator import Decorator,DecoratorProperty
from kalamar.property import Property
from ..common import run_common, make_site


class SimpleDecorator(Decorator):
    """Simple Decorator access point, wich evaluates its values"""

    def preprocess_save(self, item):
       if len(item.unsaved_properties):
           for key in item.unsaved_properties:
               try:
                   values = eval(item.unsaved_properties[key])
               except:
                   values = item.unsaved_properties.getlist(key)
               item.setlist('base_%s' % key, values)


def make_ap():
    """Create a simple access point."""
    underlying_access_point = Memory(
        {"id": Property(int), "base_name": Property(unicode)}, ("id",))
    decorated_prop = DecoratorProperty(unicode,
            lambda item: item.getlist("base_name"))
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
        site.create('things', {'id': 1,
            'name': '("foobar".replace("bar",""),)',
            'base_name' : None}).save()
        site.create('things', {'id': 2,
            'name': '("foobar".replace("foo",""),)',
            'base_name': None}).save()
        site.create('things', {'id': 3,
            'name': '("foobar".replace("foo",""),)',
            'base_name': None}).save()
    test(site)

@run_common
def test_decorator_common():
    """Define a custom test runner for the common tests."""
    return None, runner, "Decorator"
