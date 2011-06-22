# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from .where import Where
from bson.code import Code


class MongoRequest(object):

    def __init__(self):
        self.current_where = Where()
        self.mapreduces = []
        self.count = False
        self.one = False
        self.fields = {}

    def __repr__(self):
        return ("MongoRequest("
        "current_where=%r, mapreduces=%r, fields=%r, count=%r, one=%r)") % (
            self.current_where(),
            self.mapreduces,
            self.fields,
            self.count,
            self.one)

    def set_current_where(self, where_expr):
        self.current_where = Where(where_expr)

    def execute(self, collection):
        print(self)
        for mr in self.mapreduces:
            collection = mr.execute(collection)
        results = collection.find(self.current_where())
        if self.count:
            return results.count()
        if self.one:
            return collection.find_one(self.current_where())
        if self.mapreduces:
            def mr_transform(results):
                for result in results:
                    yield result["value"]
            return mr_transform(results)
        else:
            return results
