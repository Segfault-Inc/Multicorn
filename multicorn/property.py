# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.


class BaseProperty(object):
    def __init__(self, name, type_=object):
        self.name = name
        self.type = type_
