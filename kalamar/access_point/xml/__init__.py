from kalamar.access_point.decorator import Decorator, DecoratorItem,\
        DecoratorProperty
from kalamar.property import to_type
from kalamar.item import AbstractItem, Item
from kalamar.request import make_request

from lxml import etree
from StringIO import StringIO



class XMLItem(DecoratorItem):
    """
    Base XMLItem
    """

    def __init__(self, access_point, wrapped_item, decorated_values=None):
        super(XMLItem, self).__init__(access_point, wrapped_item,
                decorated_values)
        self._xml_tree = None

    @property
    def xml_tree(self):
        if self._xml_tree is None:
            parser = etree.XMLParser()
            xmlstring = self[self.access_point.stream_property].read()
            if xmlstring == None or xmlstring.strip() == u'':
                root = etree.Element(self.access_point.root_element)
                self._xml_tree = etree.ElementTree(element = root)
            else:
                self._xml_tree = etree.fromstring(xmlstring, parser)
        return self._xml_tree

class XMLProperty(DecoratorProperty):

    def __init__(self, property_type, xpath, *args, **kwargs):
        super(XMLProperty, self).__init__(property_type, self.getter,
                *args, **kwargs)
        parts = xpath.strip('/').split('/')
        self.xpath = xpath
        self.tag_name = parts[-1]
        self.parent_xpath = '//' + '/'.join(parts[:-1])

    def getter(self, item):
        if self.relation == 'one-to-many':
            return self.access_point._default_loader({}, self)(item)
        nodes = self.get_nodes(item)
        if len(nodes) == 0:
            return (None,)
        if self.type == Item:
            return self.item_from_xml(nodes[0])
        elif self.type == iter:
            return ((node.text for node in nodes),)
        return self.cast(tuple(node.text for node in nodes))

    def get_nodes(self, item):
        nodes = item.xml_tree.xpath(self.xpath)
        return nodes

    def item_from_xml(self, elem):
            request = dict(((child.tag, unicode(child.text)) for child in elem))
            return (self.remote_ap.open(make_request(request)),) \
                    if len(request) else (None,)

    def to_xml(self, value):
        element = etree.Element(self.tag_name)
        def fill_elem(elem, value):
            if isinstance(value, AbstractItem):
                identity = value.identity
                for name, value in identity.conditions.items():
                    subelement = etree.Element(name)
                    fill_elem(subelement, value)
                    elem.append(subelement)
            else:
                elem.text = value
        fill_elem(element, value)
        return element

    def create_parent(self, doc):
        def inner_create_element(parent, path):
            parts = path.strip('/').split('/')
            next_tag = parts[0]
            if not len(next_tag.strip()):
                return parent
            node = parent.xpath(next_tag)
            if not len(node):
                node = etree.Element(next_tag)
                parent.append(node)
            else :
                node = node[0]
            rest = "/".join(parts[1:])
            return inner_create_element(node, rest)
        return inner_create_element(doc.getroot(), self.parent_xpath)

class XML(Decorator):

    ItemDecorator = XMLItem

    def __init__(self, wrapped_ap, decorated_properties, stream_property, root_element):
        super(XML, self).__init__(wrapped_ap, {})
        self.stream_property = stream_property
        self.root_element = root_element
        self.decorated_properties = dict(decorated_properties)
        self.ordered_decorated_properties = decorated_properties
        for key, prop in self.decorated_properties.items():
            self.register(key, prop)

    def update_xml_tree(self, item):
        for key, prop in self.ordered_decorated_properties:
            if key in item.unsaved_properties:
                value = item.unsaved_properties.get(key)
                prop = self.decorated_properties[key]
                if prop.parent_xpath == '//':
                    parent_nodes = (item.xml_tree.getroot(),)
                else:
                    parent_nodes = item.xml_tree.xpath(prop.parent_xpath)
                if len(parent_nodes) == 0:
                    parent_node = prop.create_parent(item.xml_tree)
                elif len(parent_nodes) > 1:
                    raise ValueError("The xpath is ambiguous")
                else:
                    parent_node = parent_nodes[0]
                for child in parent_node:
                    if child.tag == prop.tag_name:
                        parent_node.remove(child)
                if prop.type == iter:
                    for atom in value:
                        parent_node.append(prop.to_xml(atom))
                else:
                    parent_node.append(prop.to_xml(value))


    def preprocess_save(self, item):
        if len(item.unsaved_properties):
            self.update_xml_tree(item)
            stream = StringIO()
            stream.write(etree.tostring(item.xml_tree))
            item[self.stream_property] = stream
