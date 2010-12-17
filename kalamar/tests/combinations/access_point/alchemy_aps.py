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
Tests for Alchemy access point combinations.

"""

from kalamar.item import Item
from kalamar.access_point.alchemy import Alchemy, AlchemyProperty

from ..test_combinations import FirstAP, SecondAP

if "unicode" not in locals():
    unicode = str


URL = "sqlite:///"


@FirstAP()
def make_first_ap():
    """First access point for Memory."""
    properties = {
        "id": AlchemyProperty(int),
        "name": AlchemyProperty(unicode),
        "color": AlchemyProperty(unicode),
        "second_ap": AlchemyProperty(
            Item, relation="many-to-one", remote_ap="second_ap",
            remote_property="code")}
    return Alchemy(URL, "first_ap", properties, ["id"], True)

@SecondAP()
def make_second_ap():
    """Second access point for Memory."""
    properties = {
        "code": AlchemyProperty(unicode),
        "name": AlchemyProperty(unicode),
        "first_aps": AlchemyProperty(
            iter, relation="one-to-many", remote_ap="first_ap",
            remote_property="second_ap")}
    return Alchemy(URL, "second_ap", properties, ["code"], True)
