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
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Storage module listing all storage access points.

"""

import os
import werkzeug

from kalamar.storage.base import AccessPoint

def load():
    """Import all modules in the curent package."""
    if not load.__loaded:
        for module in werkzeug.find_modules(__name__, include_packages=True,
                                            recursive=True):
            werkzeug.import_string(module)
        load.__loaded = True
load.__loaded = False
