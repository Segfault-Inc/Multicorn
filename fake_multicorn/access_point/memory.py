from .abstract import AbstractAccessPoint


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
