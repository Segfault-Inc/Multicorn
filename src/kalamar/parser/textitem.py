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
# along with Kalamar library. If not, see <http://www.gnu.org/licenses/>.

"""TODO : put some doc here"""

from kalamar.item import AtomItem

class TextItem(AtomItem):
    """A class to access the item's data as a unicode string."""
    
    format = "text"
    
    def _read_property_from_data(self, prop_name):
        if prop_name == "_content":
            self._open()
            content =  self._stream.read()
            properties["_content"] = content.decode(self.encoding)
        else:
            properties[prop_name] = None
        
    def serialize(self):
        content = self.properties["_content"]
        return content.encode(self.encoding)

