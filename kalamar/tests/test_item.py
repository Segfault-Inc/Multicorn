# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
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
Memory test
===========

Test the Memory access point.

"""

from nose.tools import eq_

from . import test_memory
from .common import make_site


def test_modification_tracking():
    site = make_site(test_memory.make_ap(), fill=True)
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
