# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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
ReStructuredText Access Point
=============================

Access point designed to store values in a reStructuredText document.

"""

from __future__ import print_function
import os

try:
    import docutils.core
    from lxml import etree
except ImportError:
    import sys
    print("WARNING: The ReST AP is not available.", file=sys.stderr)
else:
    _XSLT_FILENAME = os.path.join(os.path.dirname(__file__), "xml2rst.xsl")
    MAIN_XSLT = etree.XSLT(etree.parse(open(_XSLT_FILENAME), etree.XMLParser()))

from kalamar.item import AbstractItem, Item
from kalamar.value import to_bytes
from . import XML, XMLItem, XMLProperty

try:
    from StringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO


TITLE = "//title"
PARAGRAPH = "//paragraph"
SECTION = "//section"


class RestItem(XMLItem):
    """Base ReST item."""
    @property
    def xml_tree(self):
        if self._xml_tree is None:
            parser = etree.XMLParser()
            docutils_tree = docutils.core.publish_doctree(
                source = self[self.access_point.stream_property].read())
            xmlstring = docutils_tree.asdom().toxml()
            if xmlstring == None or not(xmlstring.strip()):
                root = etree.Element(self.access_point.root_element)
            else:
                root = etree.fromstring(xmlstring, parser)
            self._xml_tree = etree.ElementTree(element=root)
        return self._xml_tree


class RestProperty(XMLProperty):
    """Property to be used with a ReST access point."""
    def __init__(self, property_type, xpath, *args, **kwargs):
        if property_type == Item:
            xpath = "%s/%s" % (xpath, "raw")
        super(RestProperty, self).__init__(
            property_type, xpath, *args, **kwargs)

    def to_xml(self, value):
        """Build an XML element for a given value.

        This method overrides :meth:`XMLProperty.to_xml` to create custom role
        element for docutils, which value is the reference representaion of the
        item. It will results in ReST as::

          :raw-kalamar:`reference_repr`

        """
        if isinstance(value, AbstractItem):
            elem = etree.Element(
                self.tag_name, classes="raw-kalamar", format="kalamar")
            elem.text = value.reference_repr()
            return elem
        else:
            return super(RestProperty, self).to_xml(value)

    def item_from_xml(self, elem):
        """Custom XML serializaion for item, based on ``reference_repr``."""
        return self.remote_ap.loader_from_reference_repr(elem.text)(None)


class Rest(XML):
    """ReST access point.

    Access point designed to store and access data in ReST documents. It is
    based on the XML access point, and read the document as a doctree, and
    transforms it back to ReST using an XSLT transformation.

    """
    ItemDecorator = RestItem

    def __init__(self, wrapped_ap, decorated_properties, stream_property):
        self.need_role_def = False
        super(Rest, self).__init__(
            wrapped_ap, decorated_properties, stream_property, "document")

    def register(self, name, prop):
        """Add a property to this access point.

        Overrides :meth:`AccessPoint.register` to detect when we should create
        the custom role definition.

        """
        if prop.relation is not None:
            self.need_role_def = True
        super(Rest, self).register(name, prop)

    def update_xml_tree(self, item):
        """Generate (if needed) the custom role definition."""
        if self.need_role_def:
            role_defs_nodes = item.xml_tree.xpath("//role-def")
            if not len(role_defs_nodes):
                parent = item.xml_tree.getroot()
                elem = etree.Element(
                    "role-def", classes="raw-kalamar", format="kalamar")
                parent.append(elem)
        super(Rest, self).update_xml_tree(item)

    def preprocess_save(self, item):
        if len(item.unsaved_properties):
            self.update_xml_tree(item)
            string = to_bytes(MAIN_XSLT(item.xml_tree))[:-1]
            item[self.stream_property] = StringIO(string)
