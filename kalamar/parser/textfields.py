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

r"""
Textfields parser.

This parser can store an arbitrary number of text fields into one text file.

The stored fields are named 'field1', 'field2' ... 'fieldN'
The file format use the newline ('\n') character to separate fields. It can be
escaped with the antislash, wich can be escaped again with itself.

Example:
-------
a\aa\
aaa
bbb\\
ccc
-----

will be interpreted as 3 fields: ['a\\aa\naaa', 'bbb\\', 'ccc']

"""

import sys
import re
from werkzeug import MultiDict

from textitem import TextItem

FIELDS_SEPARATOR_RE = re.compile(r'(?:(?<=\\)\\\n)|(?:(?<!\\)\n)')
NEWLINE_ESCAPING_RE = re.compile(r'(?<!\\)\\\n')
ANTISLASH_ESCAPING_RE = re.compile(ur'\\$')

class TextFieldsItem(TextItem):
    """TODO docstring"""
    format = 'textfields'
    _field_name = u'field'
    
    def _custom_parse_data(self):
        """TODO docstring"""
        properties = super(TextFieldsItem, self)._custom_parse_data()
        for i, field in enumerate(self._from_string(properties['_content'])):
            properties['%s%i' % (self._field_name, i+1)] = field
        return properties
        
    def _custom_serialize(self, properties):
        """TODO docstring"""
        # We need to 
        field_numbers = []
        for key in properties:
            if key[:len(self._field_name)] == self._field_name and \
               key[len(self._field_name):].isdigit():
                field_numbers.append(int(key[len(self._field_name):]))
        field_numbers.sort()
        field_values = [
            properties[self._field_name + str(i)]
            for i in field_numbers]
        
        properties["_content"] = self._to_string(field_values)
        return super(TextFieldsItem, self)._custom_serialize(properties)
    
    @staticmethod
    def _from_string(data):
        r"""TODO docstring

        >>> TextFieldsItem._from_string('a\\aa\\\naaa\nbbb\\\\\nccc')
        ['a\\aa\naaa', 'bbb\\', 'ccc']

        """
        fields = FIELDS_SEPARATOR_RE.split(data)
        return [NEWLINE_ESCAPING_RE.sub('\n', field) for field in fields]
        
    @staticmethod
    def _to_string(fields):
        r"""TODO docstring

        >>> TextFieldsItem._to_string(['a\\aa\naaa', 'bbb\\', 'ccc'])
        u'a\\aa\\\naaa\nbbb\\\\\nccc'

        """
        escaped_fields = []
        for field in fields:
            field = ANTISLASH_ESCAPING_RE.sub(ur'\\\\', field)
            field = field.replace(u'\n', u'\\\n')
            escaped_fields.append(field)
        return u'\n'.join(escaped_fields)
    
