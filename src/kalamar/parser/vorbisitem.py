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
    """The Ogg/Vorbis parser.
    
    The vorbis format allows a lot of things for tagging. It is possible to
    add any label you want and, for each label, to put several values.
    Because of that, this module cannot guarantee a set of properties. Despite
    this, here are some common tags you can use :
      - time_length : duration in seconds
      - _content : raw ogg/vorbis data
      - artist
      - genre
      - track
    
    """
    
    format = "audio_vorbis"
    
    def _custom_parse_data(self):
        from ogg import vorbis
        
        props["time_length"] = [v.time_total(0)]
        props["_content"] = [self._stream.read()]
        
        self._stream.seek(0)
        v = vorbis.VorbisFile(self._stream)
        props.update(v.comment().as_dic())
        return props
    
    def _serialize(self, properties):
        return self.properties["_content"][0]
    
del AtomItem
