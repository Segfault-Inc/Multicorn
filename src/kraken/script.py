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
from itertools import chain, imap
import os
import doctest
from werkzeug import script, import_string

SRC_DIR = os.path.dirname(os.path.dirname(__file__))

def list_modules(module, base=SRC_DIR):
    dirname = os.path.join(base, *module)
    for basename in os.listdir(dirname):
        path = os.path.join(dirname, basename)
        if os.path.isdir(path):
            for i in list_modules(module + (basename,), base):
                yield i
        elif basename.endswith('.py'):
            if basename == '__init__.py':
                yield module, path
            else:
                yield module + (basename[:-3],), path

def run_tests():
    for name, path in chain(*imap(list_modules, 
                                  (('kalamar',), ('kraken',), ('koral',)))):
        doctest.testmod(import_string('.'.join(name)))
        with open(path) as f:
            todo = f.read().count('TODO')
        if todo:
            print path, ':', todo, 'TODO'+('s' if todo>1 else '')
    
    

def run(site):
    action_runserver = script.make_runserver(site, use_reloader=True,
                                             use_debugger=True)
    action_shell = script.make_shell(lambda: {'site': site})
    action_test = run_tests
    script.run()

if __name__ == '__main__':
    run()

