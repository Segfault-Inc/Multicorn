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
Base template engine.

"""

import os.path


class BaseEngine(object):
    """Abstract class for all template engine adaptators in Koral.
    
    Subclasses must override the ``render`` method.

    """
    def __init__(self, path_to_root):
        """Template engine initialisation."""
        self.path_to_root = path_to_root
    
    def render(self, template_name, values={}, lang=None, modifiers=None):
        """Render ``template_name`` with the ``values`` dict, return unicode.

        This method has to be overriden.
            
        Parameters:

        - ``template_name``: path to the template file used.
        - ``values``: dictionnary of values used by the template.
        - ``lang``: lang code like "en-us" or "fr"
        - ``modifiers``: theming modifiers. These can be specific to each
          template engine.

        """
        raise NotImplementedError


class FileBasedEngine(BaseEngine):
    def _build_filename(self, template_name):
        """Convert a slash-separated template name to an absolute filename
        """
        parts = (part for part in template_name.split(u'/')
            if part and part != u'..')
        return os.path.join(self.path_to_root, *parts)

