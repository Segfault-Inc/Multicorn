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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

"""
Simple engine for Koral, based on ``str.format``.
See http://docs.python.org/library/string.html#formatstrings

"""

from . import BaseEngine


class StrFormatEngine(BaseEngine):
    """Simple Koral engine based on ``str.format``:
    http://docs.python.org/library/string.html#formatstrings
    
    This is mainly useful for testing Koral and Kraken, when other template
    engines may not be installed.

    Equivalent to ``str.format(**values)`` on the content of the template file.

    """
    name = "str-format"
    
    def __init__(self, path_to_root, encoding="utf-8"):
        """Str-format engine initialisation."""
        super(StrFormatEngine, self).__init__(path_to_root)
        self.encoding = encoding
        
    def render(self, template_name, values, lang, modifiers):
        """Render str-format template."""
        with open(self._build_filename(template_name)) as file_descriptor:
            return file_descriptor.read().decode(self.encoding).format(**values)
