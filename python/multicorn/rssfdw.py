from . import ForeignDataWrapper
from lxml import etree
import urllib

class RssFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        self.url = options['url']
        self.columns = columns


    def make_item_from_xml(self, xml_elem):
        properties = dict(
            (prop, xml_elem.xpath(prop)[0].text)
            for prop in self.columns)
        return properties


    def execute(self, quals):
        try:
            xml = etree.fromstring(urllib.urlopen(self.url).read())
            for elem in xml.xpath('//item'):
                yield self.make_item_from_xml(elem)
        except:
            return

