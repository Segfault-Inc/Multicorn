# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.
from collections import Iterable
from .abstract import AbstractCorn
from ..requests.types import Type, List
from ..requests import requests
from .. import python_executor


class EasyCorn(AbstractCorn):
    """
    This is an helper for creating corns with simple optimizations.
    """

    def filter(self, predicate):
        raise NotImplementedError("Filter is not implemented")

    def register(self, name, type=unicode):
        type = Type(corn=self, name=name, type=type)
        self.properties[name] = type

    def execute(self, request):
        chain = requests.as_chain(request)
        if len(chain) > 1 and isinstance(chain[1], requests.FilterRequest):
            filter_request = chain[1]
            predicate = object.__getattribute__(filter_request, 'predicate')
            try:
                new_filter_request = self.filter(predicate)
            except:
                return python_executor.execute(request)
            request._copy_replace({filter_request: new_filter_request})
            return request()
        return python_executor.execute(request)
