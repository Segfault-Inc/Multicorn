# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Mongo DB test.

Test the Mongo DB noSql access point.

"""

from kalamar.access_point.mongo import MongoProperty, Mongo

from ..common import make_site, run_common, require

HOSTNAME = "localhost"
PORT = 27017
DBNAME = "dbtst"
COLLECTION = "test"


def make_ap():
    """Create a simple access point."""
    return Mongo(HOSTNAME, PORT, DBNAME, COLLECTION, {
            "id": MongoProperty(int, "_id"),
            "name": MongoProperty()})

def clean_ap(access_point):
    """Suppress all ldap objects in the test path."""
    import pymongo
    connection = pymongo.Connection(access_point.hostname, access_point.port)
    db = connection[access_point.database]
    collection = db[COLLECTION]
    collection.drop()

def runner(test):
    """Test runner for ``test``."""
    access_point = make_ap()
    try:
        site = make_site(access_point, fill=not hasattr(test, "nofill"))
        test(site)
    finally:
        clean_ap(access_point)

@require("pymongo")
@run_common
def test_common():
    """Launch common tests for Mongo DB."""
    return None, runner, "MongoDB"
