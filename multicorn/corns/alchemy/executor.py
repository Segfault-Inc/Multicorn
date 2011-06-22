# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class BadQueryException(Exception):
    pass

class QueryExecutor(object):

    def __init__(self, access_point):
        self.access_point = access_point
        self.dialect = self.acces_point.dialect
        self.properties = self.access_point.properties
        self.queries = []
        self.remainder = []
        self.joins = []
        self.selects = {}


    def execute(self, query):
        for operation in query.operations:
            self.feed(operation)

    def feed_select(self, query):
        if self.queries and self.queries[-1].kind not in ('select',):
            self.remainder.append(query)
        else:
            selects = {}
            try:
                for name, expression in query.iteritems():
                    selects[name] = self.dialect.get_selectable(expression)
                self.selects = selects
            except BadQueryException:
                self.remainder.append(query)

    def feed_select_also(self, query):
        selects = self.selects
        self.feed_select(query)
        selects.update(self.selects)
        self.selects = selects

    def feed(self, query):
        if self.remainder:
            self.remainder.append(query)
        getattr(self, query.kind)(query)
