# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from collections import MutableMapping


class BaseItem(MutableMapping):
    """
    Base class for items.

    :param corn: The corn this item is coming from
    :param values: A dict of {property name: value}
    :param lazy_values: A dict of {property name: loader} where loader is a
                        callable returning the actual value. This can be used
                        for values that are expensive to compute or get.
    """
    def __init__(self, corn, values=None, lazy_values=None):
        self.corn = corn
        self._values = dict(values or {})
        self._lazy_values = dict(lazy_values or {})
        # TODO: raise if we don’t have all values.

    def __len__(self):
        return len(self.corn.properties)

    def __iter__(self):
        for prop in self.corn.properties:
            yield prop.name

    def __contains__(self, key):
        # MutableMapping.__contains__ would work but is based on __getitem__
        # which may load a lazy value needlessly.
        return any(prop.name == key for prop in self.corn.properties)

    def __getitem__(self, key):
        if key not in self._values and key in self._lazy_values:
            return self._lazy_values[key]()
        return self._values[key]

    def __setitem__(self, key, value):
        if key not in self:  # based on self.corn.properties
            raise KeyError(key, 'Can not add properties to items.')
        self._values[key] = value
        self._lazy_values.pop(key, None)

    def __delitem__(self, key):
        raise TypeError('Can not delete properties from an item.')
