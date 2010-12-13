# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
#
# This file is adapted from xml2rst
# http://www.merten-home.de/FreeSoftware/xml2rst/
# Copyright © 2009 Stefan Merten
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
XML to ReST converter.

"""

import os.path
from lxml import etree


_XSLT_FILENAME = os.path.join(os.path.dirname(__file__), "xml2rst.xsl")
MAIN_XSLT = etree.XSLT(etree.parse(open(_XSLT_FILENAME), etree.XMLParser()))


def convert(document):
    """Convert XML ``document`` into ReST."""
    # Chop off trailing linefeed - added somehow
    return str(MAIN_XSLT(document))[:-1]
