from .abstract import AbstractAccessPoint
from ..queries import isolate
from .. import queries

class Memory(AbstractAccessPoint):
    """
    A simple access point that keep Python Item objects in memory
    """
    
    def __init__(self, name, properties, ids):
        super(Memory, self).__init__(name, properties, ids)
        self._storage = {}
    
    def save(self, item):
        key = tuple(item[prop.name] for prop in self.identity_properties)
        self._storage[key] = item
        item.saved = True
    
    def _all(self):
        return self._storage.itervalues()
    
    def search(self, query=queries.Query):
        # If all identity properties have a fixed value in the query, we can
        # build the storage key.
        ids = [prop.name for prop in self.identity_properties]
        values, query_remainder = isolate.isolate_values_from_query(
            query, ids)
        try:
            key = tuple(values[name] for name in ids)
        except KeyError:
            # Fall back on normal handling
            return super(Memory, self).search(query)
        else:
            # If possible, use a fast dict lookup instead of iterating
            # through the whole storage.
            item = self._storage[key]
            return queries.execute([item], query_remainder)
