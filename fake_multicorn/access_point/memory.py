from .base import AbstractAccessPoint


class Memory(AbstractAccessPoint):
    """
    A simple access point that keep Python Item objects in memory
    """
    
    def __init__(self, name):
        super(Memory, self).__init__(name)
        self.__storage = set()
    
