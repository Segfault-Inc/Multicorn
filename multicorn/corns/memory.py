# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .abstract import AbstractCorn
from ..queries import isolate
from ..requests.types import Type
from ..requests import wrappers
from ..requests.requests import as_chain, cut_request
from .. import queries


class Memory(AbstractCorn):
    """
    A simple access point that keep Python Item objects in memory.

    The values for identity properties must be hashable (usable as a
    dictionary key.)
    """

    def __init__(self, *args, **kwargs):
        super(Memory, self).__init__(*args, **kwargs)
        self._storage = {}

    def save(self, item):
        key = tuple(item[name] for name in self.identity_properties)
        self._storage[key] = item
        item.saved = True

    def _all(self):
        return self._storage.itervalues()

    def execute(self, request):
        wrapped_request = self.RequestWrapper.from_request(request)
        chain = as_chain(wrapped_request)
        # If we filter straight away on id properties, cut the chain in
        # half and work only on the matching item
        if len(chain) > 1 and isinstance(chain[1], wrappers.FilterWrapper):
            filter, other = cut_request(wrapped_request, chain[0])
            types = chain[0].used_types
        return wrapped_request.execute((self._all(),))

    def register(self, name, type=object, **kwargs):
        self.properties[name] = Type(corn=self, name=name, type=type)
