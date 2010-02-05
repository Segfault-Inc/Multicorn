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
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

"""
Mako engine support for Koral.

"""

import warnings

from koral.engine.base import BaseEngine



try:
    from mako.lookup import TemplateLookup
except ImportError:
    warnings.warn('Can not import mako. '
                  'MakoEngine will not be available.',
                  ImportWarning)
else:
    class MakoEngine(BaseEngine):
        r"""Koral engine for Mako: http://www.makotemplates.org/
        
        >>> import koral.site, test.koral, os.path
        >>> path = os.path.join(os.path.dirname(test.koral.__file__), 'templates')
        >>> engine = koral.site.Site(path).engines['mako']
        >>> engine.render('hello.mako.html')
        u'<html><body>Hello, World!</body></html>\n'
        >>> engine.render('hello.mako.html', {'name': 'Python'})
        u'<html><body>Hello, Python!</body></html>\n'

        """
        name = 'mako'
        
        def __init__(self, *args, **kwargs):
            """Mako engine initialisation."""
            super(MakoEngine, self).__init__(*args, **kwargs)
            self._loader = TemplateLookup(directories=[self.path_to_root])
            
        def render(self, template_name, values={}, lang=None, modifiers=None):
            """Render mako template."""
            values = dict(values)
            template = self._loader.get_template(template_name)
            return template.render_unicode(**values)
