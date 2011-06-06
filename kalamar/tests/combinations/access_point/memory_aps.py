# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for Memory access point combinations.

"""

from kalamar.item import Item
from kalamar.property import Property
from kalamar.access_point.memory import Memory

from ..test_combinations import FirstAP, SecondAP



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
