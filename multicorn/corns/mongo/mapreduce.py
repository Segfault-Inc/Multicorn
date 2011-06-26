
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


def make_mr(fields, aggregate=None, where=None):
    if not aggregate:
        return make_mr_map(fields, where)
    return make_mr_groupby(fields, aggregate, where)


def make_mr_map(fields, where=None):
    with_all = False
    if 'this' in fields:
        with_all = True
        del fields["this"]
    fields_str = "fields = {"
    for field, origin in fields.items():
        fields_str = "%s%s: %s, " % (fields_str, field, origin)
    fields_str += "};"
    if with_all:
        fields_str += (
            "for (attr in this) {"
            "  if (attr != '_id') {"
            "    fields[attr] = this[attr];"
            "  }"
            "};")
    map = ("function () {"
           "  %s"
           "  emit(this._id, fields);"
           "}") % fields_str
    reduce = ("function (k, v) {"
              "  return v[0];"
              "}")
    return MapReduce(map, reduce, where)


def make_mr_len(key, aggregates, where=None):
    tuples = aggregates.items()
    if len(tuples) > 1:
        raise "WTF"
    keyname = tuples[0][0]
    map = (
        "function () {"
        "  var k = %s;"
        "  emit(k, {key: k, <group>: 1});"
        "}").replace("<group>", keyname) % key
    reduce = ("function (k, v) {"
              "  var total = 0;"
              "  for ( var i = 0; i < v.length; i++)"
              "    total += v[i].<group>;"
              "  return { key: k, <group>: total };"
              "}").replace("<group>", keyname)
    return MapReduce(map, reduce, where)

def make_mr_sum(key, name, summed, where=None):
    map = (
        "function () {"
        "  var k = %s, v = %s;"
        "  emit(k, { key: k, <group>: v});"
        "}").replace("<group>", name) % (key, summed)
    reduce = ("function (k, v) {"
              "  var total = 0;"
              "  for ( var i = 0; i < v.length; i++)"
              "    total += v[i].<group>;"
              "  return {  key: k, <group>: total };"
              "}").replace("<group>", name)

    return MapReduce(map, reduce, where)


def make_mr_groupby(key, aggregates, where=None):
    names = {}
    vals = {}
    for k, v in aggregates.items():
        if not isinstance(v, dict):
            return make_mr_len(key, aggregates, where)
        for sk, sv in v.items():
            names[sv] = k
            vals[sv] = sk

    map = (
        "function () {"
        "  var k = %s, v = %s;"
        "  emit(k, { key: k, <group>: v});"
        "}").replace("<group>", names['sum']) % (key, vals["sum"])
    reduce = ("function (k, v) {"
              "  var total = 0;"
              "  for ( var i = 0; i < v.length; i++)"
              "    total += v[i].<group>;"
              "  return {  key: k, <group>: total };"
              "}").replace("<group>", names['sum'])

    return MapReduce(map, reduce, where)
