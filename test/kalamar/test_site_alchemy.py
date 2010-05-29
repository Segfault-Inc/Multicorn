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
PostgreSQL tests.

"""

import os
import sys
import warnings
from unittest import TestCase
from test_site_common import \
    TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove,\
    TestSiteGetDescription, TestSiteCreateItem, MyTest

from kalamar import Site
from kalamar.config import jsonparser

site = Site(jsonparser.parse(os.path.join(os.path.dirname(__file__),
                             'data', 'kalamar_alchemy.json')))
site_tests = (TestSiteSearch, TestSiteOpen, TestSiteSave,
              TestSiteRemove, TestSiteGetDescription, TestSiteCreateItem)
    
class TestSite(object): 
    site = site
    def setUp(self):
        # Assume that all access points connection to the same base
        connection = self.site.access_points.values()[0].table.bind.connect()
        transac = connection.begin()
        try:
            connection.execute('DELETE FROM textes;')
            connection.execute('INSERT INTO textes SELECT * FROM textes_bak;')
            connection.execute('DELETE FROM morceaux;')
            connection.execute('INSERT INTO morceaux SELECT * FROM morceaux_bak;')
            connection.execute('DELETE FROM my_beloved_capsules;')
            connection.execute('INSERT INTO my_beloved_capsules SELECT * FROM my_beloved_capsules_bak;')
            connection.execute('DELETE FROM capsules_textes;')
            connection.execute('INSERT INTO capsules_textes SELECT * FROM capsules_textes_bak;')
            connection.execute('DELETE FROM textes_onetomany;')
            connection.execute('INSERT INTO textes_onetomany SELECT * FROM textes_onetomany_bak;')
            connection.execute('DELETE FROM generic_capsule_link;')
            connection.execute('INSERT INTO generic_capsule_link SELECT * FROM generic_capsule_link_bak;')
            transac.commit()
        except: 
            transac.rollback()
        finally:
            connection.close()

for access_point in site.access_points:
    for test in site_tests:
        cls = type(str('%s_%s' % (test.__name__, access_point)),
                   (TestSite, test, TestCase),
                   {'access_point_name': access_point})
        setattr(sys.modules[__name__], cls.__name__, cls)
