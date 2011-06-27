# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from .where import Where


class MongoRequests(object):

    def __init__(self):
        self.first_mr = True
        self.requests = [MongoRequest(self.first_mr)]

    @property
    def last(self):
        return self.requests[-1]

    def stack(self):
        if self.last.mapreduce:
            self.first_mr = False
        return self.requests.append(MongoRequest(self.first_mr))

    def execute(self, collection):
        last_rq = self.requests.pop()
        assert not last_rq.mapreduce
        for rq in self.requests:
            assert rq.mapreduce
            collection = rq.execute(collection)
        return last_rq.execute(collection, len(self.requests))

    def __repr__(self):
        return "MongoRequests(%r)" % self.requests


class MongoRequest(object):

    def __init__(self, first_mr):
        self.first_mr = first_mr
        self.where = Where()
        self.mapreduce = None
        self.count = False
        self.one = False
        self.start = 0
        self.stop = 0
        self.sort = []

    def __repr__(self):
        return ("MongoRequest("
                "where=%r, "
                "mapreduce=%r, "
                "sort=%r, "
                "count=%r, "
                "one=%r, "
                "slice=(%r, %r))"
                ) % (
            self.where(),
            self.mapreduce,
            self.sort,
            self.count,
            self.one,
            self.start,
            self.stop)

    def execute(self, collection, has_mr=True):
        if self.mapreduce:
            opts = {}
            if self.sort:
                if len(self.sort) > 1:
                    raise
                collection.ensure_index(self.sort[0][0])
                opts["sort"] = dict(self.sort)
            if self.start:
                start = collection.count() + self.start \
                        if self.start < 0 else self.start
                opts["skip"] = start
            if self.stop:
                stop = collection.count() + self.stop \
                        if self.stop < 0 else self.stop
                start = collection.count() + self.start \
                        if self.start < 0 else self.start
                opts["limit"] = stop - start
            results = self.mapreduce.execute(
                collection, self.where, not self.first_mr, **opts)
        else:
            # Last one
            results = collection.find(self.where(not self.first_mr))

            if self.sort:
                results = results.sort(self.sort)

            if self.start:
                start = results.count() + self.start \
                        if self.start < 0 else self.start
                results = results.skip(start)
            if self.stop:
                stop = results.count() + self.stop \
                        if self.stop < 0 else self.stop
                start = results.count() + self.start \
                        if self.start < 0 else self.start
                results = results.limit(stop - start)

            if self.count:
                return results.count()
            if self.one:
                if results.count() != 1:
                    # TODO
                    raise ValueError()
            if has_mr:
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
