# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from multicorn.requests.requests import ARGUMENT_NOT_GIVEN
from multicorn.requests import CONTEXT as c
from collections import MutableMapping


class AttrW(MutableMapping):

    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        return self.wrapped[name]

    def __setattr__(self, name, value):
        if name == "wrapped":
            super(AttrW, self).__setattr__(name, value)
        else:
            self.wrapped[name] = value

    def __repr__(self):
        return self.wrapped.__repr__()

    def __len__(self):
        return self.wrapped.__len__()

    def __iter__(self):
        return self.wrapped.__iter__()

    def __contains__(self, key):
        return self.wrapped.__contains__(key)

    def __getitem__(self, key):
        return self.wrapped.__getitem__(key)

    def __setitem__(self, key, value):
        return self.wrapped.__setitem__(key, value)

    def __delitem__(self, key):
        return self.wrapped.__delitem__(key)

    def save(self):
        return self.wrapped.save()

    def delete(self):
        return self.wrapped.delete()


class Pop(object):

    def __init__(self, corn):
        self.corn = corn
        self.lazy = False

    def __exec(self, pop):
        if self.lazy:
            def wgen(items):
                for item in items:
                    yield AttrW(item)
            return wgen(pop.execute())
        return [AttrW(item) for item in pop.execute()]

    def one(self, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        return AttrW(self.corn.all.filter(
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
