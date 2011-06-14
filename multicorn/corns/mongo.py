# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


from __future__ import print_function
from ..abstract import AbstractCorn

try:
    import pymongo
except ImportError:
    import sys
    print("WARNING: The Mongo DB AP is not available.", file=sys.stderr)


class MongoCorn(AbstractCorn):
    """
    Corn storing items in a Mongo DB noSql server.
    """
    __metadatas = {}

    def __init__(self, name, identity_properties,
                 hostname, port, database, collection):
        super(MongoCorn, self).__init__(name, identity_properties)
        self.hostname = hostname
        self.port = port
        self.database = database
        connection = pymongo.Connection(hostname, port)
        db = connection[database]
        self.collection = db[collection]

    def _all(self):
        """Return an iterable of all items in this access points."""
        for mongo_item in self.collection.find():
            item = {}
            for prop in self.properties.values():
                if prop.relation != "one-to-many":
                    item[prop.name] = mongo_item[prop.name]
            item = self.create(item)
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
