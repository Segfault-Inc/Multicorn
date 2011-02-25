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
