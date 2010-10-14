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
Site
====

Site class. Create one for each independent site.

"""

from koral.engine import BUILTIN_ENGINES


class Site(object):
    """Koral site."""
    def __init__(self, path_to_root):
        self.path_to_root = path_to_root
        self.engines = {}

        for name, engine_class in BUILTIN_ENGINES.items():
            self.register(name, engine_class)
    
    def register(self, name, engine_class):
        """Add an engine to this site.
        
        :param name: Identifier string for this engine. Pass the same value
            to :meth:`render` to use the registered engine.
        :param engine_class: A concrete subclass of :class:`BaseEngine`.
        
        """
        self.engines[name] = engine_class(self.path_to_root)
    
    def render(self, site_engine, template_name, values=None, lang=None,
               modifiers=None):
        """Shorthand to the engine render method."""
        return self.engines[site_engine].render(
            template_name, values or {}, lang, modifiers)
