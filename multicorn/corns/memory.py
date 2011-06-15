# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .abstract import AbstractCorn
from ..requests.types import Type
from ..requests.requests import as_chain, cut_request, FilterRequest, ContextRequest
from ..requests.helpers import split_predicate, isolate_values
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
        chain = as_chain(request)
        # If we filter straight away on id properties, cut the chain in
        # half and work only on the matching item
        if len(chain) > 1 and isinstance(chain[1], FilterRequest):
            filter, other = cut_request(request, chain[1])
            # Wrapped filter is the actual filter
            wrapped_filter = RequestWrapper.from_request(filter)
            # Split the predicate: what concerns exclusively our
            # identity properties, and what concern something else
            context = (wrapped_filter.subject.return_type().inner_type,)
            id_types = [self.properties[name] for name in self.identity_properties]
            self_filter, other_filter = split_predicate(wrapped_filter,
                    id_types)
            # Isolate the values defined in a "Eq" comparison
            # Remainder is a query containing nor "Eq" comparison used
            # in the filter
            values, remainder = isolate_values(self_filter.wrapped_request, context)
            # If the provided values are not sufficient to extract an
            # item, get out of here.
            if all(idprop  in values for idprop in self.identity_properties):
                # Rebuild what is not processed in the values
                remainder_query = ContextRequest().filter(remainder).filter(
                        other_filter.wrapped_request)
                if len(as_chain(other)) == 1:
                    # No need to copy replace, just substitute the value
                    # because we had nothing after the filter
                    request = remainder_query
                else:
                    request = RequestWrapper.from_request(other).copy_replace(as_chain(other)[0], remainder_query)
                # Finally fetch the item, and execute the remainding query
                # against it.
                key = tuple(values[key] for key in self.identity_properties)
                items = (self._storage.get(key, None),)
                return self.RequestWrapper.from_request(request).execute((items,))
        return wrapped_request.execute((self._all(),))

    def register(self, name, type=object, **kwargs):
        self.properties[name] = Type(corn=self, name=name, type=type)
