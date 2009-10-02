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

import warnings

from koral.engine.base import BaseEngine

try:
    from jinja2 import Environment, FileSystemLoader
except ImportError:
    warnings.warn('Can not import jinja2. '
                  'Jinja2Engine will not be available.',
                  ImportWarning)
else:    
    class Jinja2Engine(BaseEngine):
        """Koral engine for Jinja2: http://jinja.pocoo.org/2/
        
        >>> import koral.site, test.koral, os.path
        >>> path = os.path.join(os.path.dirname(test.koral.__file__), 'templates')
        >>> engine = koral.site.Site(path).engines['jinja2']
        >>> engine.render('hello.jinja2.html')
        u'<html><body>Hello, World!</body></html>'
        >>> engine.render('hello.jinja2.html', {'name': 'Python'})
        u'<html><body>Hello, Python!</body></html>'

        """
        name = 'jinja2'
        
        def __init__(self, *args, **kwargs):
            """Jinja 2 engine initialisation."""
            super(Jinja2Engine, self).__init__(*args, **kwargs)
            self._env = Environment(loader=FileSystemLoader(self.path_to_root))
            
        def render(self, template_name, values={}, lang=None, modifiers=None):
            """Render Jinja 2 template."""
            template = self._env.get_template(template_name)
            return template.render(**values)

