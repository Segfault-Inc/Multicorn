# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for Alchemy access point combinations.

"""

from multicorn.item import Item
from multicorn.access_point.alchemy import Alchemy, AlchemyProperty

from ..test_combinations import FirstAP, SecondAP
from ...common import require


URL = "sqlite:///"


@FirstAP()
@require("sqlalchemy")
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
@require("sqlalchemy")
def make_second_ap():
    """Second access point for Memory."""
    properties = {
        "code": AlchemyProperty(unicode),
        "name": AlchemyProperty(unicode),
        "first_aps": AlchemyProperty(
            iter, relation="one-to-many", remote_ap="first_ap",
            remote_property="second_ap")}
    return Alchemy(URL, "second_ap", properties, ["code"], True)
