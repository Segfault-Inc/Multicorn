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
Tests for Ldap access point combinations.

"""

from kalamar.item import Item
from kalamar.access_point.mongo import MongoProperty, Mongo

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
