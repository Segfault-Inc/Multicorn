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

import os

dir = os.path.dirname(__file__)
ls = [ os.path.splitext(name)[0]
    for name in os.listdir(dir)
    if os.path.isfile(dir+os.path.sep+name)
    and name[0] != "_"
    and os.path.splitext(name)[1] == ".py"]

for name in ls:
    __import__(name, globals(), locals())

del dir
del os
del ls
