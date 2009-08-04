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
import os
from koral.engine.base import BaseEngine

try:
    from genshi.template import TemplateLoader
    import genshi.input
except ImportError:
    warnings.warn('Can not import genshi. '
                  'GenshiEngine will not be available.')
else:
    class GenshiEngine(BaseEngine):
        r"""Koral engine for Genshi: http://genshi.edgewall.org/
        
        >>> import koral.site, test.koral, os.path
        >>> path = os.path.join(os.path.dirname(test.koral.__file__), 'templates')
        >>> engine = koral.site.Site(path).engines['genshi']
        >>> engine.render('hello.genshi.html')
        ...   # doctest: +NORMALIZE_WHITESPACE
        u'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">\n<html><body>Hello,
        World!</body></html>'
        >>> engine.render('hello.genshi.html', {'name': 'Python'})
        ...   # doctest: +NORMALIZE_WHITESPACE
        u'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">\n<html><body>Hello,
        Python!</body></html>'
        """
        
        name = 'genshi'
        
        def __init__(self, *args, **kwargs):
            """Genshi engine initialisation."""
            super(GenshiEngine, self).__init__(*args, **kwargs)
            self._loader = TemplateLoader(self.path_to_root, auto_reload=True)
            
        def render(self, template_name, values={}, lang=None, modifiers=None):
            """Render genshi template."""
            values = self.make_values(values)
            stream = self._loader.load(template_name).generate(**values)
            return stream.render(method='xhtml', encoding=None, doctype='xhtml')
        
        def make_values(self, values):
            return dict(values, XML=genshi.input.XML)
            

