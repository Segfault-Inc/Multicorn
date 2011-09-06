# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from .base import BaseItem
from collections import Iterable


class MultiValueItem(BaseItem):
    """A simple extension to base item in order to support MultiDict behaviour.
    Provides getlist and setlist methods
    """
    def __init__(self, corn, values=None, lazy_values=None):
        super(MultiValueItem, self).__init__(corn, values, lazy_values)
        self._values = {}
        for key, value in values.items():
            self._values[key] = (tuple(value)
                                 if not isinstance(value, basestring) and
                                 isinstance(value, Iterable)
                                 else (value,))

    def __getitem__(self, key):
        """Get the first value of the tuple of values associated to ``key``."""
        return self.getlist(key)[0]

    def __setitem__(self, key, value):
        """Set ``(value,)`` as the tuple of values associated to ``key``."""
        self.setlist(key, (value,))

    def getlist(self, key):
        """Get the tuple of values associated to ``key``."""
        return tuple(super(MultiValueItem, self).__getitem__(key))

    def setlist(self, key, values):
        """Set the ``values`` tuple of values associated to ``key``."""
        super(MultiValueItem, self).__setitem__(key, tuple(values))

    def itemslist(self):
        """Like items() but with the list."""
        return [(key, self.getlist(key)) for key in self.keys()]

    def valueslist(self):
        """Like values() but with the list."""
        return [self.getlist(key) for key in self.keys()]

    def iteritemslist(self):
        """Like iteritems() but with the list."""
        return ((key, self.getlist(key)) for key in self.keys())

    def itervalueslist(self):
        """Like itervalues() but with the list."""
        return (self.getlist(key) for key in self.keys())
