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
Koral Site class.

Create one for each independent site.

"""

import os

from koral import engine, utils



class Site(object):
    """Koral site."""
    def __init__(self, path_to_root):
        """Create a kalamar site."""
        self.path_to_root = path_to_root
        self.engines = {}

        engine.load()
        for subclass in utils.recursive_subclasses(engine.BaseEngine):
            if getattr(subclass, 'name', None):
                self.engines[subclass.name] = subclass(self.path_to_root)
    
    def render(self, engine, template_name, values={}, lang=None,
               modifiers=None):
        """Shorthand to the engine render method.
        
        TODO test & doc

        """
        return self.engines[engine].render(
            template_name, values, lang, modifiers)

