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


import os.path

from koral.engine.base import BaseEngine


class PythonEngine(BaseEngine):
    """TODO: doc & doctest
    """
    
    name = 'py'
    
    def __init__(self, *args, **kwargs):
        super(PythonEngine, self).__init__(*args, **kwargs)
        self._module_cache = {}
    
    def get_template(self, template_name):
        filename = os.path.join(self.path_to_root, *template_name.split('/'))
        mtime = os.stat(filename).st_mtime
        try:
            module, old_mtime = self._module_cache[template_name]
        except KeyError:
            pass
        else:
            if mtime == old_mtime:
                return module

        globals_ = {}
        locals_ = {}
        execfile(filename, globals_, locals_)
        self._module_cache[template_name] = (locals_, mtime)
        return locals_
        
    def render(self, template_name, values={}, lang=None, modifiers=None):
        template = self.get_template(template_name)
        return template['render'](values)

