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

import os.path
from nose.tools import eq_
import kalamar
from kalamar.property import Property
from kalamar.access_point.filesystem import FileSystem


def test_filesytem_init():
    # Project root. Contains kalamar dir.
    root = os.path.dirname(os.path.dirname(kalamar.__file__))
    ap = FileSystem(root, '*/tests/test_*.py*',
                    ['package', ('module', Property(unicode)), 'extension'])
    site = kalamar.Site()
    site.register('tests', ap)
    eq_(set(ap.properties.keys()),
        set(['package', 'module', 'extension', 'content']))
    eq_(set(ap.identity_properties), set(['package', 'module', 'extension']))
    
    this = {'package': 'kalamar', 'module': 'filesystem', 'extension': ''}
    filename = __file__[:-1] if __file__.endswith('.pyc') else __file__
    eq_(ap._filename_for(this), filename)
    
    f = site.open('tests', this)['content']
    eq_(f.name, filename)
    # This test tests it’s own presence!
    assert 'RANDOM STRING A6buCMTbAdCV98j00vK455UIAPCJ' in f.read()
    

