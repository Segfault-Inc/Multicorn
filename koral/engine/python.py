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
Python engine support for Koral.

"""

import sys
import imp

from koral.engine.base import BaseEngine


class PythonEngine(BaseEngine):
    """Python engine for Koral.

    Simply calls ``handle_request(request)``.

    """
    name = 'py'
    
    def render(self, template_name, values={}, lang=None, modifiers=None):
        """Render Python template."""
        sys.path.insert(0, self.path_to_root)
        open_file, file_name, description = imp.find_module(template_name[:-3])
        module = imp.load_module(template_name[:-3], open_file, file_name, description)
        sys.path.pop(0)        
        return module.handle_request(values["request"])
