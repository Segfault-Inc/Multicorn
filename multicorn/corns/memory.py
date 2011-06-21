# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .abstract import AbstractCorn
from ..requests.types import Type
from ..requests.requests import as_chain, FilterRequest
from ..requests.helpers import isolate_identity_values, cut_on_predicate
from ..requests.wrappers import RequestWrapper


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
        # If we filter straight away on id properties, cut the chain in
        # half and work only on the matching item
        filter, other = cut_on_predicate(request,
                lambda x : isinstance(x, FilterRequest),
                position=0)
        if filter:
            id_types = [self.properties[name] for name in self.identity_properties]
            values, remainder_query = isolate_identity_values(filter, id_types)
            if all(idprop  in values for idprop in self.identity_properties):
                # Rebuild what is not processed in the values
                if len(as_chain(other)) == 1:
                    # No need to copy replace, just substitute the value
                    # because we had nothing after the filter
                    request = remainder_query
                else:
                    request = RequestWrapper.from_request(other).copy_replace({as_chain(other)[0]: remainder_query})
                # Finally fetch the item, and execute the remainding query
                # against it.
                key = tuple(values[key] for key in self.identity_properties)
                items = (self._storage.get(key, None),)
                return self.RequestWrapper.from_request(request).execute((items,))
        return wrapped_request.execute((self._all(),))

    def register(self, name, type=object, **kwargs):
        self.properties[name] = Type(corn=self, name=name, type=type)
