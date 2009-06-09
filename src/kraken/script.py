#!/usr/bin/env python
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

from werkzeug import script

import kraken.site

def run():
    action_runserver = script.make_runserver(kraken.site.Site, use_reloader=True,
                                             use_debugger=True)
    action_shell = script.make_shell(lambda: {'app': kraken.site.Site()})
    
    script.run()

if __name__ == '__main__':
    run()

