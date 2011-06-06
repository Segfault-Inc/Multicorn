# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Aliases
=======

Access point giving other names to the properties of the wrapped access point.

"""

from . import AccessPointWrapper
from ..item import MultiMapping, MultiDict, ItemWrapper
from ..request import Condition, And, Or, Not


class AliasedItem(ItemWrapper):
    """Item with aliased properties."""
    def _translate_key(self, key):
        """Get value of possibly aliased property called ``key``."""
        if key not in self.access_point.properties:
            raise KeyError("%s property is not in wrapped item." % key)
        return self.access_point.aliases.get(key, key)

    def getlist(self, key):
        return self.wrapped_item.getlist(self._translate_key(key))

    def setlist(self, key, values):
        return self.wrapped_item.setlist(self._translate_key(key), values)

    def __iter__(self):
        for key in self.wrapped_item:
            yield self.access_point.reversed_aliases.get(key, key)


class Aliases(AccessPointWrapper):
    """Wrapper access point renaming properties."""
    ItemWrapper = AliasedItem

    def __init__(self, wrapped_ap, aliases):
        """Create an access point aliasing ``wrapped_ap`` properties.

        :param wrapped_ap: Access point whose items must be aliased.
        :param aliases: Mapping where keys are the new property names,
            and values are the names in the wrapped access point.

        """
        super(Aliases, self).__init__(wrapped_ap)
        self.aliases = aliases
        self.reversed_aliases = dict(
            (value, key) for key, value in self.aliases.items())
        self.properties = {}
        for name, prop in wrapped_ap.properties.items():
            self.register(self.reversed_aliases.get(name, name), prop.copy())
        self._identity_property_names = tuple(
            self.reversed_aliases.get(prop.name, prop.name)
            for prop in wrapped_ap.identity_properties)
        self.__identity_properties = None

    def _alias_request(self, request):
        """Translate ``request`` to use aliases."""
        if isinstance(request, And):
            return And(*(self._alias_request(req)
                         for req in request.sub_requests))
        elif isinstance(request, Or):
            return Or(*(self._alias_request(req)
                        for req in request.sub_requests))
        elif isinstance(request, Not):
            return Not(self._alias_request(request.sub_request))
        else:
            name = request.property.name
            return Condition(self.aliases.get(name, name),
                             request.operator,
                             request.value)

    def search(self, request):
        return super(Aliases, self).search(self._alias_request(request))

    def delete_many(self, request):
        super(Aliases, self).delete_many(self._alias_request(request))

    def create(self, properties=None, lazy_loaders=None):
        if not properties:
            properties = {}
        if not lazy_loaders:
            lazy_loaders = {}

        if isinstance(properties, MultiMapping):
            # ``properties`` is not a dict, it has a ``getlist`` method
            # pylint: disable=E1103
            props = MultiDict()
            for key in properties:
                props.setlist(
                    self.aliases.get(key, key), properties.getlist(key))
            # pylint: enable=E1103
        else:
            props = dict((self.aliases.get(key, key), value)
                for key, value in dict(properties).items())
        lazy_loaders = dict(
            (self.aliases.get(key, key), value)
            for key, value in dict(lazy_loaders).items())
        return super(Aliases, self).create(props, lazy_loaders)
