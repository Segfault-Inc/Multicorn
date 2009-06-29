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

class BaseEngine(object):
    """Base class for all template engine adaptators in Koral.
    
    This method should be inherited and his descendant must define the following
    methods:
      - __call__(template_name, values, lang, modifiers)
        where:
          - ``template_name'' is the name of the template used to render the
            values (i.e.: "kid" or "jinja")
    
    """
