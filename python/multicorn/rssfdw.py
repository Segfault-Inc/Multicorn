"""An RSS foreign data wrapper"""

from . import ForeignDataWrapper
from datetime import datetime, timedelta
from lxml import etree
import urllib
from logging import ERROR
from multicorn.utils import log_to_postgres
import pickle

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
        for prop in self.columns:
            value = xml_elem.xpath(prop, namespaces=namespaces)
            if value:
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
            xml = etree.fromstring(urllib.urlopen(self.url).read())
            items = [self.make_item_from_xml(elem, xml.nsmap)
                     for elem in xml.xpath('//item')]
            self.cache = (datetime.now(), items)
            return items
        except etree.ParseError:
            log_to_postgres("Malformed xml, returning nothing")
            return
