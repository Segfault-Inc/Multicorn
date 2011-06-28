# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from multicorn.requests.requests import ARGUMENT_NOT_GIVEN


class Pop(object):

    def __init__(self, corn):
        self.corn = corn

    def one(self, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        return self.corn.all.filter(
            predicate, **kwargs).one().execute()

    def get(self, predicate=ARGUMENT_NOT_GIVEN, **kwargs):
        return self.corn.all.filter(
            predicate, **kwargs).execute()


