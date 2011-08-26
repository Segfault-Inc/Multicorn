
# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from bson.code import Code
from .ragequit import RageQuit
from multicorn.utils import highlight_js
from logging import getLogger


class MapReduce(object):

    def __init__(self, map, reduce, finalize=None):
        self.log = getLogger('multicorn')
        self.map = map
        self.reduce = reduce
        self.finalize = finalize

    def execute(self, collection, where, in_value=False, **kwargs):
        if "skip" in kwargs:
            skip = kwargs.pop("skip")
            kwargs["scope"] = {"skippy": 0}
            self.map = self.map.replace(
                "function () {",
                "function () { if(++skippy > %d) {" % skip) + "}"

        mapjs = Code(
            self.map.replace(
                "this.", "this.value.").replace(
                "this.value._id", "this._id")) \
                if in_value else Code(self.map)
        reducejs = Code(
            self.reduce.replace(
                "this.", "this.value.").replace(
                "this.value._id", "this._id")) \
                     if in_value else Code(self.reduce)

        self.log.debug(highlight_js(mapjs))

        if self.finalize:
            finalizejs = Code(self.finalize)
            kwargs.setdefault('finalize', finalizejs)
            self.log.debug(highlight_js(finalizejs))

        results = collection.map_reduce(
            mapjs,
            reducejs,
            "mr",
            query=where(in_value),
            **kwargs)
        return results

    def __repr__(self):
        return ("MapReduce(map=%r, reduce=%r)") % (
            self.map,
            self.reduce)


def make_mr_map(wrapper, fields):
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
    reduce = "function () {}"
    return MapReduce(map, reduce)


def make_mr_len(wrapper, key, aggregates):
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
    return MapReduce(map, reduce)


def make_mr_sum(wrapper, key, name, summed):
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

    return MapReduce(map, reduce)


def make_mr_max(wrapper, key, name, maxed):
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

    return MapReduce(map, reduce)


def make_mr_min(wrapper, key, name, mined):
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

    return MapReduce(map, reduce)


def make_mr_list_groupby(grouper, alias):
    map = "function () {"
    map += "  var k = %s, fields = {};" % grouper
    map += "  for (attr in this) {"
    map += "     if (attr != '_id') {"
    map += "        fields[attr] = this[attr];"
    map += "     }"
    map += "  }"
    map += "  emit(k, fields);"
    map += "}"

    reduce = "function (k, v) {"
    reduce += "  return { key: k, %s: v }" % alias
    reduce += "}"

    finalize = "function (k, v) {"
    finalize += "  if (!v.hasOwnProperty('key'))"
    finalize += "    v = { key: k, %s:[v]};" % alias
    finalize += "  return v;"
    finalize += "}"

    return MapReduce(map, reduce, finalize)


def make_mr_groupby(wrapper, grouper, aggregates):
    aggregations = {}
    level1 = aggregates.values()[0]

    if not isinstance(level1, dict):
        if level1 == 'len':
            return make_mr_len(wrapper, grouper, aggregates)

    for key, value in aggregates.items():
        if not isinstance(value, dict) and 'this' in value:
            return make_mr_list_groupby(grouper, key)

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
    finalize = None
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

    return MapReduce(map, reduce, finalize)
