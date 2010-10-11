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
Genshi engine support for Koral.

"""

from . import BaseEngine


class GenshiEngine(BaseEngine):
    """Koral engine for Genshi: http://genshi.edgewall.org/"""
    name = "genshi"
    
    def __init__(self, *args, **kwargs):
        """Genshi engine initialisation."""
        super(GenshiEngine, self).__init__(*args, **kwargs)
        from genshi.template import TemplateLoader
        self._loader = TemplateLoader(self.path_to_root, auto_reload=True)
        
    def render(self, template_name, values, lang, modifiers):
        """Render Genshi template."""
        import genshi.input
        values = dict(values, XML=genshi.input.XML)
        stream = self._loader.load(template_name).generate(**values)
        return stream.render(method="html", encoding=None, doctype="html5")
