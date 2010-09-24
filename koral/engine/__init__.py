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
Parser module listing all engine access points.

"""

from .python import PythonEngine
from .str_format import StrFormatEngine
from .jinja2_ import Jinja2Engine
from .genshi_ import GenshiEngine
from .mako_ import MakoEngine


BUILTIN_ENGINES = {'py': PythonEngine, 'str-format': StrFormatEngine}

for name, engine in (('jinja2', Jinja2Engine), ('genshi', GenshiEngine),
                     ('mako', MakoEngine)):
    try:
        engine('/nonexistent-path')
    except ImportError:
        # not installed/available
        pass
    else:
        BUILTIN_ENGINES[name] = engine

