# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class Fields(object):

    def __init__(self):
        self.fields = {"_id": 0}

    def show(self, field):
        if field.startswith("this."):
            field = field[5:]
        self.fields[field] = 1

    def hide(self, field):
        if field.startswith("this."):
            field = field[5:]
        self.fields[field] = 0

    def __call__(self):
        return self.fields if self.fields != {"_id": 0} else None

    def __repr__(self):
        return "%r" % self()
