# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class Where(object):

    def __init__(self, where=""):
        self.where = where

    def __call__(self, in_value=False):
        if not self.where:
            return {}
        return {"$where": self.where.replace("this.", "this.value.")} \
               if in_value else {"$where": self.where}

    def __repr__(self):
        return "%r" % self()
