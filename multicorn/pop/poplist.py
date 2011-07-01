# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from collections import Iterable


class PopList(list):

    def __getattr__(self, name):
        return PopList(getattr(elt, name) for elt in self)

    def __setattr__(self, name, value):
        if not isinstance(value, Iterable):
            value = [value]
        for index in range(len(self)):
            val = value[index] if index < len(value) else value[len(value) - 1]
            setattr(self[index], name, val)
