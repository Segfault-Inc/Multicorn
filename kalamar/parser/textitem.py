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
# along with Kalamar. If not, see <http://www.gnu.org/licenses/>.

"""
Text access point.

This parser is useful to parse plain text files. It can also be used to create
new access points for plain text based formats.

"""

import sys

from kalamar.item import AtomItem
from werkzeug import MultiDict

class TextItem(AtomItem):
    """Access item data as a unicode string."""
    format = 'text'
    
    def _custom_parse_data(self):
        """Parse and decode data according to encoding."""
        content =  self._stream.read()
        properties = {}
        properties["_content"] = content.decode(self.encoding)
        return properties
        
    def _custom_serialize(self, properties):
        """Return an encoded string representing the object."""
        content = properties['_content']
        return content.encode(self.encoding)
