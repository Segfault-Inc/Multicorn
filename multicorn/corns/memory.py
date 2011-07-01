# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .abstract import AbstractCorn
from ..requests.types import Type
from ..requests.requests import as_chain, FilterRequest
from ..requests.helpers import isolate_identity_values, cut_on_index
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

    def _key(self, item):
        return tuple(item[name] for name in self.identity_properties)

    def save(self, *args):
        for item in args:
            key = self._key(item)
            self._storage[key] = dict(item)
            item.saved = True

    def delete(self, item):
        key = self._key(item)
        del self._storage[key]
        item.saved = False # XXX ?

    def _all(self):
        for values in self._storage.itervalues():
            yield self.create(values)

    def execute(self, request):
        wrapped_request = self.RequestWrapper.from_request(request)
        # If we filter straight away on id properties, cut the chain in
        # half and work only on the matching item
        chain = as_chain(request)
        if len(chain) > 1 and isinstance(chain[1], FilterRequest):
            filter, other = cut_on_index(request, 1)
            id_types = [
                self.properties[name] for name in self.identity_properties]
            values, remainder_query = isolate_identity_values(filter, id_types)
            if all(idprop  in values for idprop in self.identity_properties):
                # Rebuild what is not processed in the values
                if len(as_chain(other)) == 1:
                    # No need to copy replace, just substitute the value
                    # because we had nothing after the filter
                    request = remainder_query
                else:
                    wrapped = RequestWrapper.from_request(other)
                    request = wrapped._copy_replace({
                        as_chain(other)[0]: remainder_query})
                # Finally fetch the item, and execute the remainding query
                # against it.
                key = tuple(values[key] for key in self.identity_properties)
                item = self._storage.get(key, None)
                if item is None:
                    items = []
                else:
                    items = [self.create(item)]
                wrapped = self.RequestWrapper.from_request(request)
                return wrapped.execute((items,))
        return wrapped_request.execute((self._all(),))

    def register(self, name, type=object, **kwargs):
        self.properties[name] = Type(corn=self, name=name, type=type)
