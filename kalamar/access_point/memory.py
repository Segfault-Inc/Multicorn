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
Memory
======

Access point storing items in memory. Mainly useful for testing.

"""

from . import AccessPoint


class Memory(AccessPoint, set):
    """Trivial access point that keeps everything in memory.

    Mainly useful for testing.

    """
    def search(self, request):
        return (item for item in self if request.test(item))

    def delete(self, item):
        self.remove(item)

    def delete_many(self, request):
        # build a temporary set as we can not delete (change the set size)
        # during iteration
        for item in set(self.search(request)):
            self.delete(item)

    def save(self, item):
        item.saved = True
        self.add(item)
