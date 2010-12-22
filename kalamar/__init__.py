# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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
Kalamar - Content Management Library
====================================

Kalamar offers tools to access, create and edit data stored in heterogeneous
storage engines, with a simple key/value interface.

The storage engines are called "access points". Some access points map common
low-level storages such as databases or filesystems. Other access points can
manage, atop low-level access points, high-level features such as cache, keys
aliases or data formats.

Different access points can be listed in a Kalamar "site", and relations can be
defined between these access points, just as in a relational database. This
mechanism automatically links the items stored in the linked access points,
enabling the user to easily use joins if needed.

"""

if "unicode" in __builtins__:
    __builtins__["bytes"] = str
else:
    __builtins__["unicode"] = __builtins__["basestring"] = str
