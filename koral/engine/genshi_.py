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

from koral.engine.base import BaseEngine

class GenshiEngine(BaseEngine):
    """
    Koral engine for Genshi: http://genshi.edgewall.org/
    
    >>> import koral.site, test.koral, os.path
    >>> path = os.path.join(os.path.dirname(test.koral.__file__), 'templates')
    >>> engine = koral.site.Site(path).get_engine('genshi')
    >>> print engine.render('hello.genshi.html')
    ...   # doctest: +NORMALIZE_WHITESPACE
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN"
                          "http://www.w3.org/TR/html4/strict.dtd">
    <html><body>Hello, World!</body></html>
    >>> print engine.render('hello.genshi.html', {'name': 'Python'})
    ...   # doctest: +NORMALIZE_WHITESPACE
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN"
                          "http://www.w3.org/TR/html4/strict.dtd">
    <html><body>Hello, Python!</body></html>
    """
    
    name = 'genshi'
    
    def __init__(self, *args, **kwargs):
        super(GenshiEngine, self).__init__(*args, **kwargs)
        from genshi.template import TemplateLoader
        self._loader = TemplateLoader(self.path_to_root, auto_reload=True)
        
    def render(self, template_name, values={}, lang=None, modifiers=None):
        template = self._loader.load(template_name)
        return template.generate(**values).render('html', doctype='html')


