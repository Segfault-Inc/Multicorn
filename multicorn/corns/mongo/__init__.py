# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


from __future__ import print_function
from multicorn import colorize
from multicorn.requests.types import Type, Dict, List
from ... import python_executor
from ..abstract import AbstractCorn
from .wrapper import MongoWrapper

try:
    import pymongo
except ImportError:
    import sys
    print(colorize(
        'yellow',
        "WARNING: The Mongo DB AP is not available."), file=sys.stderr)


class Mongo(AbstractCorn):
    """
    Corn storing items in a Mongo DB noSql server.
    """

    def __init__(self, name, identity_properties,
                 hostname, port, database, collection):
        super(Mongo, self).__init__(name, identity_properties)
        self.hostname = hostname
        self.port = port
        self.database = database
        self.collection_name = collection
        self.register("_id", int)

    def bind(self, multicorn):
        super(Mongo, self).bind(multicorn)
        if not hasattr(self.multicorn, '_mongo_metadatas'):
            self.multicorn._mongo_metadatas = {}
        connect_point = "%s:%s" % (self.hostname, self.port)
        connection = self.multicorn._mongo_metadatas.get(connect_point, None)
        if connection is None:
            connection = pymongo.Connection(self.hostname, self.port)
            self.multicorn._mongo_metadatas[connect_point] = connection
        self.connection = connection
        self.db = self.connection[self.database]
        self.collection = self.db[self.collection_name]

    def register(self, name, type=object, **kwargs):
        self.properties[name] = Type(
            corn=self, name=name, type=type)

    def _all(self):
        """Return an iterable of all items in the mongo collection."""
        for mongo_item in self.collection.find():
            yield self._mongo_to_item(mongo_item)

    def delete(self, item):
        self.collection.remove(
            dict((key, value) for key, value in item.items()))

    def save(self, item):
        self.collection.save(dict(
            (key, value) for key, value in item.items()
            if not (key == "_id" and value is None)))
        item.saved = True

    def is_all_mongo(self, request):
        used_types = request.used_types()
        all_requests = reduce(
            lambda x, y: list(x) + list(y), used_types.values(), set())
        return all(
            isinstance(x, MongoWrapper) for x in all_requests)

    def _mongo_to_item(self, mongo_item):
        item = {}
        for name in self.properties.keys():
            item[name] = mongo_item[name]
        return self.create(item)

    def _execute(self, expression, return_type):
        if isinstance(return_type, List):
            result = self.collection.find(
                expression.spec, expression.fields \
                if expression.fields else None)
            if isinstance(return_type.inner_type, Dict):
                return [
                    self._mongo_to_item(mongo_item) for mongo_item in result]
            else:
                return [mongo_item[expression.fields.keys()[0]] for mongo_item in result]
        else:
            if expression.one:
                return self._mongo_to_item(
                    self.collection.find_one(expression.spec))
            result = self.collection.find(expression.spec)
            if expression.count:
                return result.count()
            raise
            return [self._mongo_to_item(mongo_item) for mongo_item in result]

    def execute(self, request):
        wrapped_request = MongoWrapper.from_request(request)
        if self.is_all_mongo(wrapped_request):
            return self._execute(
                wrapped_request.to_mongo(),
                wrapped_request.return_type())
        else:
            return python_executor.execute(request)
