from . import XML, XMLItem, XMLProperty

from kalamar.item import AbstractItem, Item

import docutils.core

from lxml import etree
from xml2rst import rst_xslt
from StringIO import StringIO

TITLE = '//title/'

PARAGRAPH = '//paragraph/'

SECTION = '//section/'


class RestItem(XMLItem):
    """
    Base RestItem
    """
    @property
    def xml_tree(self):
        if self._xml_tree is None:
            parser = etree.XMLParser()
            docutils_tree = docutils.core.publish_doctree(
                source = self[self.access_point.stream_property].read())
            xmlstring = docutils_tree.asdom().toxml()
            if xmlstring == None or xmlstring.strip() == u'':
                root = etree.Element(self.access_point.root_element)
                self._xml_tree = etree.ElementTree(element = root)
            else:
                self._xml_tree = etree.ElementTree(element = etree.fromstring(xmlstring, parser))
        return self._xml_tree

class RestProperty(XMLProperty):

    def __init__(self, property_type, xpath, *args, **kwargs):
        if property_type == Item:
            xpath = "%s/%s" % (xpath, "raw")
        super(RestProperty, self).__init__(property_type, xpath, *args, **kwargs)

    def to_xml(self, value):
        if isinstance(value, AbstractItem):
            elem = etree.Element(self.tag_name, classes="raw-kalamar",
                    format="kalamar")
            elem.text = value.reference_repr()
            return elem
        else:
            return super(RestProperty, self).to_xml(value)

    def item_from_xml(self, elem):
        return self.remote_ap.loader_from_reference_repr(elem.text)(None)



class Rest(XML):

    ItemDecorator = RestItem

    def __init__(self, wrapped_ap, decorated_properties, stream_property):
        self.need_role_def = False
        super(Rest, self).__init__(wrapped_ap, decorated_properties,
                stream_property, 'document')


    def register(self, name, prop):
        if prop.relation is not None:
            self.need_role_def = True
        super(Rest, self).register(name, prop)

    def update_xml_tree(self, item):
        if self.need_role_def:
            role_defs_nodes = item.xml_tree.xpath('//role-def')
            if not len(role_defs_nodes):
                parent = item.xml_tree.getroot()
                elem = etree.Element('role-def', classes='raw-kalamar', format='kalamar')
                parent.append(elem)
        super(Rest, self).update_xml_tree(item)

    def preprocess_save(self, item):
        if len(item.unsaved_properties):
            self.update_xml_tree(item)
            item[self.stream_property] = StringIO(rst_xslt.convert(item.xml_tree))
