"""An RSS foreign data wrapper"""

from . import ForeignDataWrapper
from lxml import etree
import urllib


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
        self.url = options['url']
        self.columns = columns

    def make_item_from_xml(self, xml_elem):
        """Internal method used for parsing item xml element from the
        columns definition."""
        properties = dict(
            (prop, xml_elem.xpath(prop)[0].text)
            for prop in self.columns)
        return properties

    def execute(self, quals):
        """Quals are ignored."""
        try:
            xml = etree.fromstring(urllib.urlopen(self.url).read())
            for elem in xml.xpath('//item'):
                yield self.make_item_from_xml(elem)
        except etree.ParseError:
            print("Malformed xml, returning nothing")
            return
