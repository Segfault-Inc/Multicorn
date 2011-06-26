
# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from bson.code import Code
from .where import Where
from .ragequit import RageQuit


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


def make_mr_map(wrapper, fields, where=None):
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


def make_mr_len(wrapper, key, aggregates, where=None):
    tuples = aggregates.items()
    if len(tuples) > 1:
        raise RageQuit(wrapper, "WTF")
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


def make_mr_sum(wrapper, key, name, summed, where=None):
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


def make_mr_max(wrapper, key, name, maxed, where=None):
    map = (
        "function () {"
        "  var k = %s, v = %s;"
        "  emit(k, { key: k, <group>: v});"
        "}").replace("<group>", name) % (key, maxed)
    reduce = ("function (k, v) {"
              "  var max = v[0].<group>;"
              "  for ( var i = 1; i < v.length; i++)"
              "    if ( v[i].<group> > max )"
              "      max = v[i].<group>;"
              "  return {  key: k, <group>: max };"
              "}").replace("<group>", name)

    return MapReduce(map, reduce, where)


def make_mr_min(wrapper, key, name, mined, where=None):
    map = (
        "function () {"
        "  var k = %s, v = %s;"
        "  emit(k, { key: k, <group>: v});"
        "}").replace("<group>", name) % (key, mined)
    reduce = ("function (k, v) {"
              "  var min = v[0].<group>;"
              "  for ( var i = 1; i < v.length; i++)"
              "    if ( v[i].<group> < min )"
              "      min = v[i].<group>;"
              "  return {  key: k, <group>: min };"
              "}").replace("<group>", name)

    return MapReduce(map, reduce, where)


def make_mr_groupby(wrapper, grouper, aggregates, where=None):
    aggregations = {}
    level1 = aggregates.values()[0]
    if not isinstance(level1, dict):
        return make_mr_len(wrapper, grouper, aggregates, where)
    level2 = level1.values()[0]
    if isinstance(level2, dict):
        raise RageQuit(wrapper, "Too much level in groupy")
    for key, value in aggregates.items():
        for op, aggregated in value.items():
            if isinstance(aggregated, dict):
                new_aggregate = aggregates.values()[0]
                return make_mr_groupby(grouper, new_aggregate)
            aggregations[op] = {"alias": key, "aggregated": aggregated}

    map = "function () {"
    map += "  var k = %s;" % grouper
    map += "  emit(k, { key: k, "
    for op, agg in aggregations.items():
        map += "%s: %s, " % (
            agg["alias"],
            agg["aggregated"]
            )
    map += "});"
    map += "}"

    return_ = " return { key: k, "
    reduce = "function (k, v) {"
    if "sum" in aggregations:
        sum = aggregations["sum"]
        reduce += "  var sum = 0;"
        reduce += "  for ( var i = 0; i < v.length; i++)"
        reduce += "    sum += v[i].%s;" % sum["alias"]
        return_ += "%s: sum, " % sum["alias"]
    if "min" in aggregations:
        min = aggregations["min"]
        reduce += "  var min = v[0].%s;" % min["alias"]
        reduce += "  for ( var i = 1; i < v.length; i++)"
        reduce += "    if ( v[i].%s < min )" % min["alias"]
        reduce += "      min = v[i].%s;" % min["alias"]
        return_ += "%s: min, " % min["alias"]
    if "max" in aggregations:
        max = aggregations["max"]
        reduce += "  var max = v[0].%s;" % max["alias"]
        reduce += "  for ( var i = 1; i < v.length; i++)"
        reduce += "    if ( v[i].%s > max )" % max["alias"]
        reduce += "      max = v[i].%s;" % max["alias"]
        return_ += "%s: max, " % max["alias"]
    return_ += " };"
    reduce += return_
    reduce += "}"

    return MapReduce(map, reduce, where)
