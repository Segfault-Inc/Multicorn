# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from ..abstract import AbstractCorn


class AbstractCornExtenser(AbstractCorn):

    def __init__(self, name, wrapped_corn):
        self.name = name
        self.multicorn = None
        self.wrapped_corn = wrapped_corn
        self.identity_properties = tuple(self.wrapped_corn.identity_properties)
        self.properties = dict(self.wrapped_corn.properties)

    def bind(self, multicorn):
        super(AbstractCornExtenser, self).bind(multicorn)
        self.wrapped_corn.bind(multicorn)

    # def __getattr__(self, key):
    #     return getattr(self.wrapped_corn, key)
