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


from kalamar.item import Item

class TextItem(Item):
    """Access item data as a unicode string."""
    format = 'text'
    
    def _custom_parse_data(self):
        """Parse and decode data according to encoding."""
        properties = super(TextItem, self)._custom_parse_data()
        properties['text'] = self._get_content().decode(self.encoding)
        return properties
        
    def _custom_serialize(self, properties):
        """Return an encoded string representing the object."""
        return self['text'].encode(self.encoding)
