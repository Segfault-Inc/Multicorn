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
Test access point.

This access point is internally used for testing purpose.

"""

from kalamar.parser.textitem import TextItem

class TestItem(TextItem):
    """Test access point item."""
    format = 'test_format'
    _keys = ('genre', 'artist', 'album', 'tracknumber', 'title')
    
    def _custom_parse_data(self):
        """Parse known properties of the test item."""
        properties = super(TestItem, self)._custom_parse_data()
        data = properties['_content']
        properties.update(dict(zip(self._keys, data.split('\n'))))
        if properties['tracknumber']:
            properties['tracknumber'] = int(properties['tracknumber'])
        else:
            properties['tracknumber'] = None
        return properties
        
    def _custom_serialize(self, properties):
        """Return a string of properties representing the test item."""
        properties['_content'] = u'\n'.join(
            unicode(properties.get(key, u'')) for key in self._keys)
        return super(TestItem, self)._custom_serialize(properties)
