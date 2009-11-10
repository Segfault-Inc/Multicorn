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
Database tests helpers.

"""

from test_site_common import \
    TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove,\
    TestSiteGetDescription, TestSiteCreateItem

class TestSite(object):
    def setUp(self):
        # Assume that all access points connect to the same base
        connection = self.site.access_points.values()[0].get_connection()[0]
        cursor = connection.cursor()
        try:
            cursor.execute('DELETE FROM textes;')
            cursor.execute('INSERT INTO textes SELECT * FROM textes_bak;')
            cursor.execute('DELETE FROM morceaux;')
            cursor.execute('INSERT INTO morceaux SELECT * FROM morceaux_bak;')
            connection.commit()
        finally:
            cursor.close()

tests = (TestSiteSearch, TestSiteOpen, TestSiteSave,
         TestSiteRemove, TestSiteGetDescription, TestSiteCreateItem)
