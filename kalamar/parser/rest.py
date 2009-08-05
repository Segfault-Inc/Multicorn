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
ReStructured Text.
"""

try:
    import docutils.core
    import docutils.nodes
except ImportError:
    import warnings
    warnings.warn('Can not import docutils. '
                  'RestItem will not be available.')
else:
    from kalamar.item import AtomItem
    import re
    
    def extract_includes(text):
        r"""
        Return a list of included filenames in the given ReST string.
        
        >>> extract_includes(u'''
        ... ===============
        ... A test document
        ... ===============
        ... 
        ... .. include:: nonexistent.rst
        ... 
        ... Etiam a turpis erat, ac scelerisque nisl. Pellentesque habitant
        ... morbi tristique senectus et netus et malesuada fames ac turpis
        ... egestas. 
        ... 
        ... .. include:: foo/bar.rst
        ... .. .. include:: commented.rst
        ... 
        ... ''')
        [u'nonexistent.rst', u'foo/bar.rst']
        """
        return [match.group(1) for match in extract_includes._re.finditer(text)]
    extract_includes._re = re.compile(u'^\s*.. include::\s+(\S+)\s*$',
                                      re.MULTILINE)
        
        
    def extract_metadata(text):
        r"""
        Return a dict of metadata for the given ReST string.
        
        Search for a docutils.nodes.title and a docutils.nodes.field_list
        element in the docutils document tree.
        
        >>> sorted(extract_metadata(u'''
        ... ===============
        ... A test document
        ... ===============
        ... :date: 2009-08-04
        ... :abstract: Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        ...            Suspendisse fringilla accumsan sem eget ullamcorper.
        ...            Aliquam erat volutpat.
        ... 
        ... .. include:: nonexistent.rst
        ... 
        ... Etiam a turpis erat, ac scelerisque nisl. Pellentesque habitant
        ... morbi tristique senectus et netus et malesuada fames ac turpis
        ... egestas. Donec fringilla, nisl in viverra sagittis, elit arcu 
        ... velit nulla quis ligula.
        ... 
        ... ''').items()) # doctest: +NORMALIZE_WHITESPACE
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
                if fields:
                    break
            elif isinstance(element, docutils.nodes.field_list):
                for field_name, field_body in element:
                    fields[field_name.astext()] = field_body.astext()
                if title:
                    break
        fields[u'title'] = title
        return fields

    class RestItem(AtomItem):
        """TODO docstring

        """
        format = 'rest'
        
        def _custom_parse_data(self):
            """Parse docutils metadata as properties."""
            properties = super(RestItem, self)._custom_parse_data()
            properties.update(extract_metadata(properties['_content']))
            return properties
        


