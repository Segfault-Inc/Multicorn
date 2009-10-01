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
reStructuredText converter.

TODO docstring

"""

import docutils.core

def rest_to_html(rest_data, filename=None, **kwargs):
    """Convert ReST to HTML.

    Convert a reStructured to HTML, not including <html> and <body> tags.
    If given, ``filename`` is used to resolve relative includes.

    """
    parts = docutils.core.publish_parts(
        source=rest_data,
        source_path=filename,
        writer_name="html",
        settings_overrides=kwargs)
    return parts['fragment']
