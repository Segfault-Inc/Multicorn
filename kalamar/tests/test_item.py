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
Item test.

Test the Item class.

"""

from kalamar.access_point.memory import Memory
from kalamar.property import Property
from .common import make_site


def memory_make_ap():
    """Create a simple access point."""
    return Memory({"id": Property(int), "name": Property(unicode)}, "id")

def test_modification_tracking():
    """Test the modification tracking system."""
    # Some statements seem useless here, but they are useful
    # pylint: disable=W0104
    site = make_site(memory_make_ap(), fill=True)
    item = tuple(site.search("things"))[0]
    assert not item.modified
    item["name"] = "spam"
    assert item.modified
    item.save()
    assert not item.modified
    item["name"]
    assert not item.modified
    item.getlist("name")
    assert not item.modified
    item.setlist("name", ("spam", "egg"))
    assert item.modified
    item.save()
    assert not item.modified
    # pylint: enable=W0104
