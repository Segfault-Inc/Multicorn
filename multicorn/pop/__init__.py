# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from multicorn.requests.requests import ARGUMENT_NOT_GIVEN, AttributeRequest
from multicorn.requests import CONTEXT as c
from .attrwrap import AttrWrap


class Pop(object):

    def __init__(self, corn):
        self.corn = corn
        self.pop = self.corn.all
        self.lazy = False

    def _filter(self, filter, **kwargs):
        self.pop = self.pop.filter(filter, **kwargs)

    def _map(self, map):
        self.pop = self.pop.map(map)

    def _sort(self, sort):
        if sort:
            if isinstance(sort, AttributeRequest):
                self.pop = self.pop.sort(sort)
            else:
                self.pop = self.pop.sort(*sort)

    def _exec(self):
        if self.lazy:
            def wgen(items):
                for item in items:
                    yield AttrWrap(item)
            return wgen(self.pop.execute())
        return [AttrWrap(item) for item in self.pop.execute()]

    def one(self, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        self._filter(predicate, **kwargs)
        return AttrWrap(self.pop.one().execute())

    def get(self, filter=ARGUMENT_NOT_GIVEN, sort=None):
        self._filter(filter)
        self._sort(sort)
        return self._exec()

    def alias(self, aliases,
              filter=ARGUMENT_NOT_GIVEN,
              sort=None):
        self._map(aliases)
        self._filter(filter)
        self._sort(sort)
        return self._exec()


class LazyPop(Pop):

    def __init__(self, corn):
        super(LazyPop, self).__init__(corn)
        self.lazy = True
