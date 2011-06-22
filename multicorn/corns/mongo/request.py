# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from .where import Where
from .fields import Fields
from bson.code import Code


class MongoRequest(object):

    def __init__(self):
        self.current_where = Where()
        self.current_fields = Fields()
        self.mapreduces = []
        self.count = False
        self.one = False

    def __repr__(self):
        return ("MongoRequest("
                "current_where=%r, "
                "current_fields=%r, "
                "mapreduces=%r, "
                "count=%r, "
                "one=%r)") % (
            self.current_where(),
            self.current_fields(),
            self.mapreduces,
            self.count,
            self.one)

    def set_current_where(self, where_expr):
        self.current_where = Where(where_expr)

    def pop_where(self):
        old_where = self.current_where
        self.current_where = Where()
        return old_where

    def execute(self, collection):
        print(self)
        for mr in self.mapreduces:
            collection = mr.execute(collection,
                                    len(self.mapreduces) > 1)
        results = collection.find(
            self.current_where(self.mapreduces),
            self.current_fields())
        if self.count:
            return results.count()
        if self.one:
            if results.count() != 1:
                # TODO
                raise ValueError()
            else:
                return next(results)
        if self.mapreduces:
            def mr_transform(results):
                for result in results:
                    yield result["value"]
            return mr_transform(results)
        return results
