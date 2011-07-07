# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class Multicorn(object):
    def __init__(self):
        self.corns = {}

    def register(self, corn):
        corn.bind(self)  # Do this first as it may raise.
        corns = list(self.corns)
        self.corns[corn.name] = corn
        for corn in corns:
            corn.registration()
        return corn

