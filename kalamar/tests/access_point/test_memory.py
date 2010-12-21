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
Memory test.

Test the Memory access point.

"""

from kalamar.access_point.memory import Memory
from kalamar.property import Property

from ..common import run_common



def make_ap():
    """Create a simple access point."""
    return Memory({"id": Property(int), "name": Property(unicode)}, ("id",))

@run_common
def test_common():
    """Launch common tests for memory."""
    return make_ap()
