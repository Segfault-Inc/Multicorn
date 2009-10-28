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
Mail parser.

This parser is based on python builtin email module.

"""

from kalamar.parser.textitem import TextItem
import email



class MessageItem(TextItem):
    """Parse email messages using python builtin email module."""
    format = 'mail'
    
    def _custom_parse_data(self):
        """Parse email headers as properties."""
        properties = super(MessageItem, self)._custom_parse_data()
        msg = email.message_from_string(properties['_content'])
        msg.set_charset('utf-8')
        properties['message'] = msg
        return properties
        
    def _custom_serialize(self, properties):
        properties['_content'] = properties['message'].as_string()
        return super(TestItem, self)._custom_serialize(properties)
