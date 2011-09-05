# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .abstract import AbstractCorn
from ..requests import requests
from .. import python_executor


class EasyCorn(AbstractCorn):
    """
    This is an helper for creating corns with simple optimizations.
    """

    def filter(predicate):
        raise NotImplementedError("Filter is not implemented")

    def execute(self, request):
        # chain = requests.as_chain(request)
        # if len(chain) > 1 and isinstance(chain[1], requests.FilterRequest):
        #     filter_request = chain[1]
        #     predicate = object.__getattribute__(filter_request, 'predicate')
        #     self.filter(predicate)
        return python_executor.execute(request)
