# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from .where import Where


class MongoRequest(object):

    def __init__(self):
        self.current_where = Where()
        self.mapreduces = []
        self.count = False
        self.one = False
        self.sort = []

    def __repr__(self):
        return ("MongoRequest("
                "current_where=%r, "
                "mapreduces=%r, "
                "sort=%r, "
                "count=%r, "
                "one=%r)") % (
            self.current_where(),
            self.mapreduces,
            self.sort,
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
            self.current_where(self.mapreduces))
        if self.sort:
            results = results.sort(self.sort)
        if self.count:
            return results.count()
        if self.one:
            if results.count() != 1:
                # TODO
                raise ValueError()
        if self.mapreduces:
            if self.one:
                return next(results)["value"]
            else:
                def mr_transform(results):
                    for result in results:
                        yield result["value"]
                return mr_transform(results)
        if self.one:
            return next(results)
        return results
