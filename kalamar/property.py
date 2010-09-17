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
Kalamar property object.

"""

import decimal
import io

from . import item


PROPERTY_TYPES = set((
        str, int, float, decimal.Decimal, io.IOBase, item.Item))


class Property(object):
    def __init__(self,  property_type, identity=False, auto=False,
                 default=None, mandatory=False, relation=None, remote_ap=None,
                 remote_property=None):
        self.property_type = property_type
        self.identity = identity
        self.auto = auto
        self.default = default
        self.remote_ap = remote_ap
        self.mandatory = mandatory
        self.relation = relation
        self.remote_property = remote_property

        # Raise an exception if something is wrong in the relation
        if self.relation:
            if not self.remote_ap:
                raise RuntimeError("Invalid property definition")
            if self.relation == "one-to-many":
                if not self.remote_property:
                    raise RuntimeError("Invalid property definition")
