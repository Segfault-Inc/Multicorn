"""
Purpose
-------

This fdw can be used to access items from an rss feed.
The column names are mapped to the elements inside an item.
An rss item has the following strcture:

.. code-block:: xml

    <item>
      <title>Title</title>
      <pubDate>2011-01-02</pubDate>
      <link>http://example.com/test</link>
      <guid>http://example.com/test</link>
      <description>Small description</description>
    </item>

You can access every element by defining a column with the same name. Be
careful to match the case! Example: pubDate should be quoted like this:
``pubDate`` to preserve the uppercased ``D``.


.. api_compat::
    :read:

Dependencies
------------

You will need the `lxml`_ library.

.. _lxml: http://lxml.de/

Required options
-----------------

``url`` (string)
  The RSS feed URL.

Usage Example
-------------

.. _Radicale: http://radicale.org

If you want to parse the `radicale`_ rss feed, you can use the following
definition:

.. code-block:: sql

    CREATE SERVER rss_srv foreign data wrapper multicorn options (
        wrapper 'multicorn.rssfdw.RssFdw'
    );

    CREATE FOREIGN TABLE radicalerss (
        "pubDate" timestamp,
        description character varying,
        title character varying,
        link character varying
    ) server rss_srv options (
        url     'http://radicale.org/rss/'
    );

    select "pubDate", title, link from radicalerss limit 10;

.. code-block:: bash

           pubDate       |              title               |                     link
    ---------------------+----------------------------------+----------------------------------------------
     2011-09-27 06:07:42 | Radicale 0.6.2                   | http://radicale.org/news#2011-09-27@06:07:42
     2011-08-28 13:20:46 | Radicale 0.6.1, Changes, Future  | http://radicale.org/news#2011-08-28@13:20:46
     2011-08-01 08:54:43 | Radicale 0.6 Released            | http://radicale.org/news#2011-08-01@08:54:43
     2011-07-02 20:13:29 | Feature Freeze for 0.6           | http://radicale.org/news#2011-07-02@20:13:29
     2011-05-01 17:24:33 | Ready for WSGI                   | http://radicale.org/news#2011-05-01@17:24:33
     2011-04-30 10:21:12 | Apple iCal Support               | http://radicale.org/news#2011-04-30@10:21:12
     2011-04-25 22:10:59 | Two Features and One New Roadmap | http://radicale.org/news#2011-04-25@22:10:59
     2011-04-10 20:04:33 | New Features                     | http://radicale.org/news#2011-04-10@20:04:33
     2011-04-02 12:11:37 | Radicale 0.5 Released            | http://radicale.org/news#2011-04-02@12:11:37
     2011-02-03 23:35:55 | Jabber Room and iPhone Support   | http://radicale.org/news#2011-02-03@23:35:55
    (10 lignes)
"""

from . import ForeignDataWrapper
from datetime import datetime, timedelta
from lxml import etree
try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen
from logging import ERROR, WARNING
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
        self.default_namespace_prefix = options.pop(
            'default_namespace_prefix', None)
        self.item_root = options.pop('item_root', 'item')

    def get_namespaces(self, xml):
        ns = dict(xml.nsmap)
        if None in ns:
            ns[self.default_namespace_prefix] = ns.pop(None)
        return ns

    def make_item_from_xml(self, xml_elem):
        """Internal method used for parsing item xml element from the
        columns definition."""
        item = {}
        for prop, column in self.columns.items():
            value = xml_elem.xpath(
                prop, namespaces=self.get_namespaces(xml_elem))
            if value:
                if column.type_name.startswith('json'):
                    item[prop] = json.dumps([
                        element_to_dict(val) for val in value])
                # There should be a better way
                # oid is 1009 ?
                elif column.type_name.endswith('[]'):
                    item[prop] = [elem.text for elem in value]
                else:
                    item[prop] = getattr(value[0], 'text', value[0])
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
            items = [self.make_item_from_xml(elem)
                     for elem in xml.xpath(
                         '//%s' % self.item_root,
                         namespaces=self.get_namespaces(xml))]
            self.cache = (datetime.now(), items)
            return items
        except etree.ParseError:
            log_to_postgres("Malformed xml, returning nothing")
        except IOError:
            log_to_postgres("Cannot retrieve '%s'" % self.url, WARNING)
