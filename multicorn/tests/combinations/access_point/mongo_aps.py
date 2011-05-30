# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for Ldap access point combinations.

"""

from multicorn.item import Item
from multicorn.access_point.mongo import MongoProperty, Mongo

from ..test_combinations import FirstAP, SecondAP
from ...common import require
from ...access_point.test_mongo import clean_ap

HOSTNAME = "localhost"
PORT = 27017
DBNAME = "dbtst"
COLLECTION = "test"
COLLECTION2 = "test2"

@FirstAP(teardown=clean_ap)
@require("pymongo")
def make_first_ap():
    """First access point for Mongo DB."""
    return Mongo(HOSTNAME, PORT, DBNAME, COLLECTION, {
            "id": MongoProperty(int, "_id"),
            "name": MongoProperty(),
            "color": MongoProperty(),
            "second_ap": MongoProperty(
                Item, relation="many-to-one", remote_ap="second_ap",
                remote_property="code")})

@SecondAP(teardown=clean_ap)
@require("pymongo")
def make_second_ap():
    """Second access point for Mongo DB."""
    return Mongo(HOSTNAME, PORT, DBNAME, COLLECTION2, {
            "code": MongoProperty(nosql_name="_id"),
            "name": MongoProperty(),
            "first_aps": MongoProperty(
                iter, relation="one-to-many", remote_ap="first_ap",
                remote_property="second_ap")})
