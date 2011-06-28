# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from multicorn.requests.requests import ARGUMENT_NOT_GIVEN
from multicorn.requests import CONTEXT as c
from .attrwrap import AttrWrap


class Pop(object):

    def __init__(self, corn):
        self.corn = corn
        self.lazy = False

    def __exec(self, pop):
        if self.lazy:
            def wgen(items):
                for item in items:
                    yield AttrWrap(item)
            return wgen(pop.execute())
        return [AttrWrap(item) for item in pop.execute()]

    def one(self, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        return AttrWrap(self.corn.all.filter(
            predicate, **kwargs).one().execute())

    def get(self, filter=ARGUMENT_NOT_GIVEN,
            sort=ARGUMENT_NOT_GIVEN):
        pop = self.corn.all.filter(filter)
        if sort != ARGUMENT_NOT_GIVEN:
            pop = pop.sort(sort)
        return self.__exec(pop)

    def alias(self, aliases,
              filter=ARGUMENT_NOT_GIVEN,
              sort=ARGUMENT_NOT_GIVEN):
        pop = self.corn.all.map(
            aliases).filter(
            filter)
        if sort != ARGUMENT_NOT_GIVEN:
            pop = pop.sort(sort)
        return self.__exec(pop)

    def add(self, aliases,
              filter=ARGUMENT_NOT_GIVEN,
              sort=ARGUMENT_NOT_GIVEN):
        pop = self.corn.all.map(
            aliases).filter(
            c + filter)
        if sort != ARGUMENT_NOT_GIVEN:
            pop = pop.sort(sort)
        return self.__exec(pop)


class LazyPop(Pop):

    def __init__(self, corn):
        super(LazyPop, self).__init__(corn)
        self.lazy = True
