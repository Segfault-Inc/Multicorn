# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class Where(object):

    def __init__(self, where=""):
        self.where = where

    def __call__(self):
        return {"$where": self.where} if self.where else {}

    def __repr__(self):
        return "%r" % self()
