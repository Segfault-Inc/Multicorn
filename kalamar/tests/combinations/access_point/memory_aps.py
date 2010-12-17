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
Tests for Memory access point combinations.

"""

from kalamar.item import Item
from kalamar.property import Property
from kalamar.access_point.memory import Memory

from ..test_combinations import FirstAP, SecondAP

if "unicode" not in locals():
    unicode = str


@FirstAP()
def make_first_ap():
    """First access point for Memory."""
    properties = {
        "id": Property(int),
        "name": Property(unicode),
        "color": Property(unicode),
        "second_ap": Property(
            Item, relation="many-to-one", remote_ap="second_ap",
            remote_property="code")}
    return Memory(properties, ["id"])

@SecondAP()
def make_second_ap():
    """Second access point for Memory."""
    properties = {
        "code": Property(unicode),
        "name": Property(unicode),
        "first_aps": Property(
            iter, relation="one-to-many", remote_ap="first_ap",
            remote_property="second_ap")}
    return Memory(properties, ["code"])
