# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from bson.code import Code
from .where import Where


class MapReduce(object):

    def __init__(self, map, reduce, where=None):
        self.map = map
        self.reduce = reduce
        self.where = where if where else Where()

    def execute(self, collection, in_value=False):
        mapjs = Code(self.map.replace("this.", "this.value.")) \
                     if in_value else Code(self.map)
        reducejs = Code(self.reduce.replace("this.", "this.value.")) \
                     if in_value else Code(self.reduce)
        results = collection.map_reduce(
            mapjs,
            reducejs,
            "mr",
            query=self.where())
        return results

    def __repr__(self):
        return ("MapReduce(map=%r, reduce=%r, where=%r)") % (
            self.map,
            self.reduce,
            self.where)


def make_mr_map(fields, where=None):
    with_all = False
    if '*' in fields:
        with_all = True
        del fields["*"]
    fields_str = "fields = {"
    for field, origin in fields.items():
        fields_str = "%s%s: %s, " % (fields_str, field, origin)
    fields_str += "};"
    if with_all:
        fields_str += (
            "for (attr in this) {"
            "if (attr != '_id') {"
            "  fields[attr] = this[attr];"
            "}};")
    map = ("function () {"
           "%s"
           "emit(this._id, fields);"
           "}") % fields_str
    reduce = ("function (k, v) {"
              "return v[0];"
              "}")
    return MapReduce(map, reduce, where)
