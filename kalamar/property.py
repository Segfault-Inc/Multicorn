# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Property
========

Kalamar property object.

"""

from .value import PROPERTY_TYPES, to_type


class MissingRemoteAP(RuntimeError):
    """Remote access point is missing in property definition."""
    value = "remote_ap missing in property definition"


class MissingRemoteProperty(RuntimeError):
    """Remote property is missing in property definition."""
    value = "remote_property missing in property definition"


class Property(object):
    def __init__(self, property_type, identity=False, auto=False,
                 default=None, mandatory=False, relation=None, remote_ap=None,
                 remote_property=None):
        self.type = property_type
        self.identity = identity
        self.auto = auto
        self.default = default
        self.remote_ap = remote_ap
        self.mandatory = mandatory
        self.relation = relation
        self.remote_property = remote_property

        # Raise an exception if something is wrong in the relation
        if self.relation and not self.remote_ap:
            raise MissingRemoteAP()
        if self.relation == "one-to-many" and not self.remote_property:
            raise MissingRemoteProperty()

    def cast(self, values):
        """Cast an iterable of values, return a tuple of cast values."""
        if not self.mandatory and values == (None,):
            return values
        if self.type in PROPERTY_TYPES:
            return tuple(PROPERTY_TYPES[self.type](value) for value in values)
        else:
            return tuple(to_type(value, self.type) for value in values)
