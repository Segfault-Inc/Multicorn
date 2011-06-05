from .base import AbstractAccessPoint


class Memory(AbstractAccessPoint):
    """
    A simple access point that keep Python Item objects in memory
    """
    
    def __init__(self, name, properties):
        super(Memory, self).__init__(name, properties)
        self.identity_properties = ()
        self.__storage = set()
    
