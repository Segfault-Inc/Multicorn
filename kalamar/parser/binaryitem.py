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
RAW access point.

This parser is mainly useful for binary files.

"""

from kalamar.item import AtomItem

class BinaryItem(AtomItem):
    """RAW access to data."""
    format = 'binary'
    
    def _custom_parse_data(self, properties):
        """Parse the whole item content."""
        properties = {}
        properties['_content'] = self._stream.read()
        
    def _custom_serialize(self, properties):
        """Return the item content."""
        return properties['_content']

del AtomItem
