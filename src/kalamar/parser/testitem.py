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
# along with Kalamar library.  If not, see <http://www.gnu.org/licenses/>.

"""TODO : put some doc here"""

from kalamar.parser.textitem import TextItem

class TestItem(TextItem):
    """A class giving the raw (binary) access to an item's data"""
    
    format = "test_format"
    
    def _custom_parse_data(self):
        props = super(TestItem, self)._custom_parse_data()
        data = props["_content"]
        props.update(dict(zip(("genre","artist","album", "title"),
                         (value for value in data.split("\n",4)))))
        return props
        
    def _serialize(self, properties):
        genre = properties["genre"]
        artist = properties["artist"]
        album = properties["album"]
        title = properties["title"]
        return '\n'.join([genre, artist, album, title])
