# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
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
ReStructuredText.

Metadata extraction using docutils is kinda slow, so the results of
``extract_metadata`` are cached.

"""

try:
    import docutils.core
    import docutils.nodes
except ImportError:
    import warnings
    warnings.warn('Can not import docutils. '
                  'RestItem will not be available.',
                  ImportWarning)
else:
    from kalamar.item import CapsuleItem
    from kalamar.parser.textitem import TextItem
    from kalamar import utils
    
    import re
    import os.path
    
    _test_document = u"""\
===============
A test document
===============
:date: 2009-08-04
:abstract: Lorem ipsum dolor sit amet, consectetur adipiscing.
           Suspendisse fringilla accumsan sem eget ullamcorper.
           Aliquam erat volutpat.

.. include:: nonexistent.rst

Etiam a turpis erat, ac scelerisque nisl. Pellentesque habitant
morbi tristique senectus et netus et malesuada fames ac turpis
egestas.

.. include:: foo/bar.rst
.. include:: name with whitespaces.rst  
.. .. include:: commented.rst

"""
    
    def extract_includes(text):
        """Return a list of included filenames in the given ReST string.
        
        >>> list(extract_includes(_test_document))
        [u'nonexistent.rst', u'foo/bar.rst', u'name with whitespaces.rst']

        """
        for match in extract_includes._re.finditer(text):
            yield match.group(1)

    extract_includes._re = re.compile(u'^\s*.. include::\s+(.+?)\s*$',
                                      re.MULTILINE)
        
    @utils.simple_cache
    def extract_metadata(text):
        """Return a dict of metadata for the given ReST string.
        
        Search for a docutils.nodes.title and a docutils.nodes.field_list
        element in the docutils document tree.
        
        >>> sorted(extract_metadata(_test_document).items())
        ... # doctest: +NORMALIZE_WHITESPACE
        [(u'abstract', u'Lorem ipsum dolor sit amet, consectetur adipiscing.
                         Suspendisse fringilla accumsan sem eget ullamcorper.
                         Aliquam erat volutpat.'),
         (u'date', u'2009-08-04'),
         (u'title', u'A test document')]

        """
        tree = docutils.core.publish_doctree(text, settings_overrides={
            'file_insertion_enabled': False, # do not follow includes
            'report_level': 3, # do not warn that they weren’t followed
            'docinfo_xform': False}) # do not interpret bibliographic fields

        title = None
        fields = {}
        for element in tree:
            # Search for a title and a field_list
            # Break when we found them both
            if isinstance(element, docutils.nodes.title):
                title = unicode(element[0])
            elif isinstance(element, docutils.nodes.field_list):
                for field_name, field_body in element:
                    fields[field_name.astext()] = field_body.astext()
            if fields and title:
                break
        fields[u'title'] = title
        return fields



    class RestAtom(TextItem):
        """This parser simply exposes ReST metadata as properties.
        
        These properties are read-only: your modifications will *not* be saved
        if you change them.

        """
        format = 'rest'
        
        def _parse_data(self):
            properties = super(TextItem, self)._parse_data()
            # assume extract_metadata never return a 'text' property
            # or the ReST text will be overwritten
            properties.update(extract_metadata(properties['text']))
            return properties
        


    class MissingItem(object):
        """Missing ReST item.

        Placeholder in RestCapsule subitems used when an ``include`` directive
        has a filename that matches no item in the current site.

        Instances evaluate to False in a boolean context so that you can
        easily test whether an Item is “missing”.
        
        >>> item = MissingItem('foo')
        >>> if item: print 'OK'
        ... else: print 'KO'
        KO

        """
        def __init__(self, filename):
            self.filename = filename
        
        def __repr__(self):
            return '<%s %r>' % (self.__class__.__name__, self.filename)
        
        def __nonzero__(self):
            return False
        


    class RestCapsule(CapsuleItem):
        """A ReStructuredText capsule.
        
        The ReST document is only made of metadata and :include: directives.
        Any other content (such as text) is discarded and will be lost when
        the capsule is saved.
        
        Metadata are exposed as properties, and :include:’s as subitems.
        The filenames are resolved to the actual kalamar items, or a MissingItem
        if no item matched the filename.

        """
        format = 'rest_capsule'
        
        def _parse_data(self):
            """Parse docutils metadata as properties."""
            properties = super(RestCapsule, self)._parse_data()
            content = self._get_content().decode(self.encoding)
            properties.update(extract_metadata(content))
            return properties

        def _load_subitems(self):
            content = self._get_content().decode(self.encoding)
            for include in extract_includes(content):
                filename = os.path.join(
                    os.path.dirname(self[u'_filename']),
                    os.path.normpath(include))
                item = self._access_point.site.item_from_filename(filename)
                # item is None if no access point has this filename
                yield item or MissingItem(include)
       
        def serialize(self):
            content = []
            write = content.append
            title = self.raw_parser_properties[u'title']
            write(u'=' * len(title))
            write(title)
            write(u'=' * len(title))
            for key, value in self.raw_parser_properties.iteritems():
                if key != u'title':
                    write(u':%s: %s' % (key, value))
            write('')
            dirname = os.path.dirname(self.filename)
            for subitem in self.subitems:
                write(u'.. include:: ' + utils.relpath(
                    subitem[u'_filename'], dirname))
            return u'\n'.join(content).encode(self.encoding)
