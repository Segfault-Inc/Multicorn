# -*- coding: utf-8 -*-
# Copyright © 2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Mongo DB
====

Access point storing items in a Mongo DB noSql server.

"""

from __future__ import print_function
from kalamar.item import Item
from kalamar.access_point import AccessPoint
from kalamar.property import Property

try:
    import pymongo
except ImportError:
    import sys
    print("WARNING: The Mongo DB AP is not available.", file=sys.stderr)

class MongoProperty(Property):
    """Property for an Mongo access point."""
    def __init__(self, property_type=unicode, nosql_name=None, **kwargs):
        super(MongoProperty, self).__init__(property_type, **kwargs)
        self.nosql_name = nosql_name
        self.name = None

class Mongo(AccessPoint):
    """Access point to a Mongo server."""

    def __init__(self, hostname, port, database, collection, properties):
        for name, prop in properties.items():
            if not prop.nosql_name:
                prop.nosql_name = name
            if prop.nosql_name == "_id":
                self._id_name = name
        if not self._id_name:
            raise KeyError("Properties list must contains an '_id'")
        super(Mongo, self).__init__(properties, (self._id_name,))
        self.hostname = hostname
        self.port = port
        self.database = database
        connection = pymongo.Connection(hostname, port)
        db = connection[database]
        self.collection = db[collection]

    def search(self, request):
        for mongo_item in self.collection.find():
            item = {}
            for prop in self.properties.values():
                if prop.relation != "one-to-many":
                    item[prop.name] = mongo_item[prop.nosql_name]
            item = self.create(item)
            if request.test(item):
                item.saved = True
                yield item

    def delete(self, item):
        nosql_dict = {}
        for key, value in item.items():
            nosql_key = self.properties[key].nosql_name
            nosql_dict[nosql_key] = value
        self.collection.remove(nosql_dict)

    def save(self, item):
        nosql_dict = {}
        for key, value in item.items():
            if self.properties[key].relation != "one-to-many":
                nosql_key = self.properties[key].nosql_name
                if self.properties[key].type == Item and value is not None:
                    nosql_dict[nosql_key] = value.reference_repr()
                else:
                    nosql_dict[nosql_key] = value
        self.collection.save(nosql_dict)
        item.saved = True
