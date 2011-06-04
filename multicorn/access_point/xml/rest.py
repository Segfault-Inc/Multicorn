# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
ReStructuredText
================

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

from multicorn.item import AbstractItem, Item
from multicorn.value import to_bytes
from . import XML, XMLItem, XMLProperty

try:
    from StringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO


# More paths can be found in the Docutils documentation:
# http://docutils.sourceforge.net/docs/ref/doctree.html
#
# Here are some useful examples:
# //topic/paragraph
# //section/title
# //hint
TITLE = "//title"
PARAGRAPH = "//paragraph"
SECTION = "//section"


_doctree_cache = {}
def memoized_publish_doctree(source):
    result = _doctree_cache.get(source)
    if result is None:
        result = docutils.core.publish_doctree(source=source)
        _doctree_cache[source] = result
    return result

_xslt_cache = {}
def memoized_xslt(tree):
    key = etree.tostring(tree)
    result = _xslt_cache.get(key)
    if result is None:
        result = to_bytes(MAIN_XSLT(tree))[:-1]
        _xslt_cache[key] = result
    return result

class RestItem(XMLItem):
    """Base ReST item."""
    @property
    def xml_tree(self):
        if self._xml_tree is None:
            parser = etree.XMLParser()
            docutils_tree = memoized_publish_doctree(
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

          :raw-multicorn:`reference_repr`

        """
        if isinstance(value, AbstractItem):
            elem = etree.Element(
                self.tag_name, classes="raw-multicorn", format="multicorn")
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
                    "role-def", classes="raw-multicorn", format="multicorn")
                parent.append(elem)
        super(Rest, self).update_xml_tree(item)

    def preprocess_save(self, item):
        if len(item.unsaved_properties):
            self.update_xml_tree(item)
            string = memoized_xslt(item.xml_tree)
            item[self.stream_property] = StringIO(string)
