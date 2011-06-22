# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from bson.code import Code
from .where import Where


class MapReduce(object):

    def __init__(self, map, reduce, where=None):
        self.map = Code(map)
        self.reduce = Code(reduce)
        self.where = where if where else Where()

    def execute(self, collection):
        print(self.map)
        print(self.reduce)
        if self.where():
            results = collection.map_reduce(
                self.map,
                self.reduce,
                "mr",
                query=self.where())
        else:
            results = collection.map_reduce(
                self.map,
                self.reduce,
                "mr")
        return results

    def __repr__(self):
        return ("MapReduce(map=%r, reduce=%r, where=%r)") % (
            self.map,
            self.reduce,
            self.where)


def aliases_mr(aliases, where=None):
    aliases_str = "{"
    for alias, origin in aliases.items():
        aliases_str = "%s%s: %s, " % (aliases_str, alias, origin)
    aliases_str += "}"
    map = ("function () {"
               "emit(this._id, %s);"
               "}") % aliases_str
    reduce = ("function (k, v) {"
              "return v[0];"
              "}")
    return MapReduce(map, reduce, where)
