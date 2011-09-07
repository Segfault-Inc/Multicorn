# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from collections import Iterable
from .abstract import AbstractCorn
from ..requests.types import Type
from ..requests import requests
from .. import python_executor


class EasyCorn(AbstractCorn):
    """
    This is an helper for creating corns with simple optimizations.
    """

    def filter(predicate):
        raise NotImplementedError("Filter is not implemented")

    def register(self, name, type=unicode):
        type = Type(corn=self, name=name, type=type)
        self.properties[name] = type

    def create(self, item):
        item = super(EasyCorn, self).create(item)
        for property, type in self.properties.items():
            if type.type != unicode:
                item[property] = type.type(item[property])
        return item

    def save(self, item):
        for property, type in self.properties.items():
            if type.type != unicode:
                item[property] = unicode(item[property]).encode('utf-8')
        self._save(item)

    def execute(self, request):
        # chain = requests.as_chain(request)
        # if len(chain) > 1 and isinstance(chain[1], requests.FilterRequest):
        #     filter_request = chain[1]
        #     predicate = object.__getattribute__(filter_request, 'predicate')
        #     self.filter(predicate)
        return python_executor.execute(request)
