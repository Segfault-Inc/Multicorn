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

from __future__ import with_statement
import os
import doctest
from werkzeug import script, import_string

SRC_DIR = os.path.dirname(os.path.dirname(__file__))

def run_tests(base=SRC_DIR, module=()):
    dirname = os.path.join(base, *module)
    for basename in os.listdir(dirname):
        path = os.path.join(dirname, basename)
        if os.path.isdir(path) and \
           os.path.isfile(os.path.join(path, '__init__.py')):
            submodule = module + (basename,)
            path = os.path.join(path, '__init__.py')
            run_tests(base, submodule)
        elif basename.endswith('.py') and basename != '__init__.py':
            submodule = module + (basename[:-3],)            
        else:
            continue
        doctest.testmod(import_string('.'.join(submodule)))
        with open(path) as f:
            todo = f.read().count('TODO')
        if todo:
            print path, ':', todo, 'TODOs'
    
    

def run(site):
    action_runserver = script.make_runserver(site, use_reloader=True,
                                             use_debugger=True)
    action_shell = script.make_shell(lambda: {'site': site})
    action_test = lambda: run_tests()
    script.run()

if __name__ == '__main__':
    run()

