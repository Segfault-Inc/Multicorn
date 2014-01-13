"""An RSS foreign data wrapper"""

from . import ForeignDataWrapper
from datetime import datetime, timedelta
from lxml import etree
try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen
from logging import ERROR
from multicorn.utils import log_to_postgres
import json


def element_to_dict(element):
    """
    This method takes a lxml element and return a json string containing
    the element attributes and a text key and a child node.
    >>> test = lambda x: sorted([(k, sorted(v.items())) if isinstance(v, dict) else (k, [sorted(e.items()) for e in v]) if isinstance(v, list) else (k, v) for k, v in element_to_dict(etree.fromstring(x)).items()])
    >>> test('<t a1="v1"/>')
    [('attributes', {'a1': 'v1'}), ('children', []), ('tag', 't'), ('text', '')]

    >>> test('<t a1="v1">Txt</t>')
    [('attributes', {'a1': 'v1'}), ('children', []), ('tag', 't'), ('text', 'Txt')]

    >>> test('<t>Txt<s1 a1="v1">Sub1</s1>Txt2<s2 a2="v2">Sub2</s2>Txt3</t>')
    [('attributes', {}), ('children', [[('attributes', {'a1': 'v1'}), ('children', []), ('tag', 's1'), ('text', 'Sub1')], [('attributes', {'a2': 'v2'}), ('children', []), ('tag', 's2'), ('text', 'Sub2')]]), ('tag', 't'), ('text', 'Txt')]

"""
    return {
        'tag': etree.QName(element.tag).localname,
        'text': element.text or '',
        'attributes': dict(element.attrib),
        'children': [element_to_dict(e) for e in element]
    }


class RssFdw(ForeignDataWrapper):
    """An rss foreign data wrapper.

    The following options are accepted:

    url --  The rss feed urls.

    The columns named are parsed, and are used as xpath expression on
    each item xml node. Exemple: a column named "pubDate" would return the
    pubDate element of an rss item.

    """

    def __init__(self, options, columns):
        super(RssFdw, self).__init__(options, columns)
        self.url = options.get('url', None)
        self.cache = (None, None)
        self.cache_duration = options.get('cache_duration', None)
        if self.cache_duration is not None:
            self.cache_duration = timedelta(seconds=int(self.cache_duration))
        if self.url is None:
            log_to_postgres("You MUST set an url when creating the table!",
                            ERROR)
        self.columns = columns

    def make_item_from_xml(self, xml_elem, namespaces):
        """Internal method used for parsing item xml element from the
        columns definition."""
        item = {}
        for prop, column in self.columns.items():
            value = xml_elem.xpath(prop, namespaces=namespaces)
            if value:
                if column.type_name.startswith('json'):
                    item[prop] = json.dumps([element_to_dict(val) for val in value])
                # There should be a better way
                # oid is 1009 ?
                elif column.type_name.endswith('[]'):
                    item[prop] = [elem.text for elem in value]
                else:
                    item[prop] = value[0].text
        return item

    def execute(self, quals, columns):
        """Quals are ignored."""
        if self.cache_duration is not None:
            date, values = self.cache
            if values is not None:
                if (datetime.now() - date) < self.cache_duration:
                    return values
        try:
            xml = etree.fromstring(urlopen(self.url).read())
            items = [self.make_item_from_xml(elem, xml.nsmap)
                     for elem in xml.xpath('//item')]
            self.cache = (datetime.now(), items)
            return items
        except etree.ParseError:
            log_to_postgres("Malformed xml, returning nothing")
            return
