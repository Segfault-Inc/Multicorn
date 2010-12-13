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

MAIN_XSLT = "xml2rst.xsl"


def convert(document):
    """Convert XML ``document`` into ReST."""
    xslt_parser = etree.XMLParser()
    with open(os.path.join(os.path.dirname(__file__), MAIN_XSLT)) as xslt:
        xslt_document = etree.parse(xslt, xslt_parser)
    main_xslt = etree.XSLT(xslt_document)
    result = main_xslt(document)
    # Chop off trailing linefeed - added somehow
    return str(result)[:-1]
