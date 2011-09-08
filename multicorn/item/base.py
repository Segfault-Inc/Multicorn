# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under a 3-clause BSD license.

from collections import MutableMapping, namedtuple
from logging import getLogger


class Identity(namedtuple("Identity", "corn, conditions")):
    """Simple class identifying items.

    :param corn: The corn name of the item.
    :param conditions: A dict of conditions identifying the item.

    >>> identity = Identity("corn_name", {"id": 1})
    >>> identity.corn
    'corn_name'
    >>> identity.conditions
    {'id': 1}

    :class:`Identity` manages equality between equivalent items.

    >>> identity2 = Identity("corn_name", {"id": 1})
    >>> identity == identity2
    True

    """


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
        self.log = getLogger('multicorn')
        self.log.debug("Creating %r item with values %r and lazy %r" % (
            corn, values, lazy_values))
        self._values = dict(values or {})
        self._lazy_values = dict(lazy_values or {})
        self.corn.properties

        corn_properties = set(self.corn.properties.keys())
        given_keys = set(self._values) | set(self._lazy_values)
        extra_keys = given_keys - corn_properties
        if extra_keys:
            raise ValueError(
                "Unexpected properties: %r" % (tuple(extra_keys),))
        missing_keys = corn_properties - given_keys
        if missing_keys:
            self.log.debug("Creating a %s with missing properties: %r" %
                      (corn, tuple(missing_keys),))
            for key in missing_keys:
                self._values[key] = None

    def __eq__(self, other):
        if not isinstance(other, BaseItem):
            return False
        return all(self[key] == other[key]
                for key in self.corn.identity_properties)

    def __repr__(self):
        return '<%s %s>' % (type(self).__name__, ', '.join(
            '%s=%r' % (key, self[key])
            for key in self.corn.identity_properties))

    def __len__(self):
        return len(self.corn.properties)

    def __iter__(self):
        return iter(self.corn.properties.keys())

    def __contains__(self, key):
        # MutableMapping.__contains__ would work but is based on __getitem__
        # which may load a lazy value needlessly.
        return key in self.corn.properties

    def __getitem__(self, key):
        if key not in self._values and key in self._lazy_values:
            return self._lazy_values[key](self)
        return self._values[key]

    def __setitem__(self, key, value):
        if key not in self:  # based on self.corn.properties
            raise KeyError(key, 'Can not add properties to items.')
        self._values[key] = value
        self._lazy_values.pop(key, None)

    def __delitem__(self, key):
        raise TypeError('Can not delete properties from an item.')

    def save(self):
        self.corn.log.debug("Saving item %r" % self)
        self.corn.save(self)

    def delete(self):
        self.corn.log.debug("Deleting item %r" % self)
        self.corn.delete(self)

    @property
    def identity(self):
        names = self.corn.identity_properties
        return Identity(
            self.corn.name, dict((name, self[name]) for name in names))

    def __cmp__(self, other):
        # TODO: test this method
        if isinstance(other, BaseItem):
            if self.identity == other.identity:
                return 0
            elif self.identity > other.identity:
                return 1
        return -1

    def __hash__(self):
        import pdb
        pdb.set_trace()
        return hash((self.corn.name,
            frozenset((prop, self[prop])
                for prop in self.corn.identity_properties)))
