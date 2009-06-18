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

from kalamar.item import AtomItem

class VorbisItem(AtomItem):
    """The Ogg/Vorbis parser."""
    
    format = "audio_vorbis"
    keys = ["artist", "album", "title", "length", "_content"]
    
    def _custom_parse_data(self):
        from ogg import vorbis
        v = vorbis.VorbisFile(self._stream)
        props = {}
        oggdic = v.comment().as_dic()
        for key in self.keys:
            props[key] = oggdic.get(key,[None])[0]
        self._stream.seek(0)
        props["_content"] = self._stream.read()
        return props
    
    def _serialize(self, properties):
        return self.properties["_content"]
        
    
del AtomItem
