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

Metadata extraction using docutils is kindda slow, so the results of
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
:abstract: Lorem ipsum dolor sit amet, consectetur adipiscing elit.
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
        r"""Return a list of included filenames in the given ReST string.
        
        >>> list(extract_includes(_test_document))
        [u'nonexistent.rst', u'foo/bar.rst', u'name with whitespaces.rst']

        """
        for match in extract_includes._re.finditer(text):
            yield match.group(1)

    extract_includes._re = re.compile(u'^\s*.. include::\s+(.+?)\s*$',
                                      re.MULTILINE)
        
    @utils.simple_cache
    def extract_metadata(text):
        r"""Return a dict of metadata for the given ReST string.
        
        Search for a docutils.nodes.title and a docutils.nodes.field_list
        element in the docutils document tree.
        
        >>> sorted(extract_metadata(_test_document).items())
        ... # doctest: +NORMALIZE_WHITESPACE
        [(u'abstract', u'Lorem ipsum dolor sit amet, consectetur 
                         adipiscing elit.\nSuspendisse fringilla accumsan sem
                         eget ullamcorper.\nAliquam erat volutpat.'),
         (u'date', u'2009-08-04'),
         (u'title', u'A test document')]

        """
        tree = docutils.core.publish_doctree(text, settings_overrides={
            'file_insertion_enabled': False, # do not follow includes
            'report_level': 3, # do not warn that they weren’t followed
            'docinfo_xform': False, # do not interpret bibliographic fields
            })

        title = None
        fields = {}
        for element in tree:
            # Search for a title and a field_list.
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


    class RestMetadataMixin(object):
        def get_content(self):
            self._open()
            self._stream.seek(0)
            return self._stream.read().decode(self.encoding)
        
        def get_metadata(self):
            """Parse docutils metadata and return a dict."""
            return extract_metadata(self.get_content())
        
    class RestAtom(RestMetadataMixin, TextItem):
        """TODO docstring

        """
        format = 'rest'
        
    class MissingItem(object):
        """Missing ReST item.

        Placeholder in RestCapsule subitems used when an ``include`` directive
        has a filename that matches no item in the current site.

        """
        def __init__(self, filename):
            self.filename = filename
        
        def __repr__(self):
            return '<%s %r>' % (self.__class__.__name__, self.filename)
        
        def __nonzero__(self):
            """
            >>> if MissingItem('foo'): print 1
            ... else: print 0
            0

            """
            return False
        
    class RestCapsule(RestMetadataMixin, CapsuleItem):
        """TODO docstring

        """
        format = 'rest_capsule'
        
        def _custom_parse_data(self):
            """Parse docutils metadata as properties."""
            properties = super(RestCapsule, self)._custom_parse_data()
            properties.update(self.get_metadata())
            return properties

        def _load_subitems(self):
            for include in extract_includes(self.get_content()):
                filename = os.path.join(
                    os.path.dirname(self.properties[u'_filename']),
                    os.path.normpath(include))
                item = self._access_point.site.item_from_filename(filename)
                # item is None if no access point has this filename
                yield item or MissingItem(include)
       
        def content_modified(self):
            return super(RestCapsule, self).content_modified() or \
                    self.subitems.modified
        
        def _custom_serialize(self, properties):
            content = []
            write = content.append
            title = self.properties[u'title']
            write(u'=' * len(title))
            write(title)
            write(u'=' * len(title))
            for key in self.properties:
                if key != u'title':
                    write(u':%s: %s' % (key, self.properties[key]))
            write('')
            # Don’t use self.properties._filename here because it may not be
            # there yet if we create a capsule from scratch
            dirname = os.path.dirname(self.filename())
            for subitem in self.subitems:
                write(u'.. include:: ' + utils.relpath(
                    subitem.properties._filename, dirname))
            return u'\n'.join(content).encode(self.encoding)
            
            


