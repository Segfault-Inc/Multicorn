# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from __future__ import print_function
from multicorn.utils import colorize, print_py
from multicorn.requests.types import Type, Dict, List
from ...requests.helpers import cut_on_predicate
from ... import python_executor
from ..abstract import AbstractCorn
from .wrappers import MongoWrapper
from .ragequit import RageQuit

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
        self.db.set_profiling_level(1)
        self.collection = self.db[self.collection_name]
        for prop in self.identity_properties:
            self.collection.ensure_index(prop)

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
        all_in = True
        for rq in all_requests:
            if not isinstance(rq, MongoWrapper):
                print(colorize("red", "%r not supported in mongo" % rq))
                all_in = False
        return all_in

    def _mongo_to_item(self, mongo_item):
        item = {}
        for name in self.properties.keys():
            item[name] = mongo_item[name]
        return self.create(item)

    def _execute(self, mrq, return_type):
        # print_py(repr(mrq))
        result = mrq.execute(self.collection)

        # Several results
        if isinstance(return_type, List):
            if isinstance(return_type.inner_type, Dict):
                if return_type.inner_type.corn:
                    def to_items(results):
                        for result in results:
                            yield self._mongo_to_item(result)
                    return to_items(result)
                else:
                    return result
            else:
                def to_list(results):
                    for mongo_item in result:
                        if "____" in mongo_item:
                            yield mongo_item["____"]
                        else:
                            yield mongo_item
                return to_list(result)
        elif isinstance(return_type, Dict):
            if return_type.corn:
                return self._mongo_to_item(result)
            else:
                return result
        elif isinstance(result, dict):
            if "____" in result:
                return result["____"]
            else:
                return result
        elif isinstance(result, int):
            return result
        result = next(result)
        if "____" in result:
            return result["____"]
        else:
            return result
        return result

    def execute(self, request):
        wrapped_request = MongoWrapper.from_request(request)
        # print_py(repr(wrapped_request))
        if self.is_all_mongo(wrapped_request):
            try:
                return self._execute(
                    wrapped_request.to_mongo(),
                    wrapped_request.return_type())
            except RageQuit as rq:
                print (colorize("red", "%r" % rq))
                if rq.request:
                    quited_on = rq.request.wrapped_request

                    def predicate(req):
                        return req is quited_on
                    managed, not_managed = cut_on_predicate(request, predicate,
                                                            recursive=True)
                    if managed:
                        result = self.execute(managed)
                    else:
                        result = self._all()
                    return python_executor.execute(not_managed, (result,))
                raise RuntimeError("WTF")
        print (colorize("yellow", "Python execution for %r" % wrapped_request))
        return python_executor.execute(request)
