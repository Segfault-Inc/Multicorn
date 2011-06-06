# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .abstract import AbstractCorn
from ..queries import isolate
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
    
    def search(self, query=queries.Query):
        # If all identity properties have a fixed value in the query, we can
        # build the storage key.
        values, query_remainder = isolate.isolate_values_from_query(
            query, self.identity_properties)
        try:
            key = tuple(values[name] for name in self.identity_properties)
        except KeyError:
            # Fall back on normal handling
            return super(Memory, self).search(query)
        else:
            # If possible, use a fast dict lookup instead of iterating
            # through the whole storage.
            item = self._storage[key]
            return queries.execute([item], query_remainder)
