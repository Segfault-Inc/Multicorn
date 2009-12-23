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
    TestSiteGetDescription, TestSiteCreateItem, MyTest



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
            cursor.execute('DELETE FROM my_beloved_capsules;')
            cursor.execute('INSERT INTO my_beloved_capsules SELECT * FROM my_beloved_capsules_bak;')
            cursor.execute('DELETE FROM capsules_textes;')
            cursor.execute('INSERT INTO capsules_textes SELECT * FROM capsules_textes_bak;')
            connection.commit()
        finally:
            cursor.close()

class TestDBCapsule(MyTest):
    def test_bestof_length(self):
        bestof_length = {}
        for bestof in self.site.search(self.access_point_name):
            bestof_length[bestof['title']] = sum(1
                for track in bestof.subitems if track)
        self.assertEquals(bestof_length, {u'Great BestOf': 3,
                                          u'Best of the Lord': 2})

    def test_create(self):
        """
        create a new capsule from stratch, save it, load it back
        and check we get it as expected
        """
        compilation = self.site.create_item(self.access_point_name,
                                            {'title': 'Compilation'})

        # Save and load back so that we get the database-generated ID
        self.site.save(compilation)
        compilation = self.site.open(self.access_point_name, 'title="Compilation"')
        
        # Make a capsule with every track
        track_titles = set()
        for bestof in self.site.search(self.access_point_name):
            for track in bestof.subitems:
                if track:
                    compilation.subitems.append(track)
                    track_titles.add(track['title'])
        self.site.save(compilation)

        compilation2 = self.site.open(self.access_point_name, 'title="Compilation"')
        self.assertEquals(track_titles, set(track['title']
            for track in compilation2.subitems))
            


site_tests = (TestSiteSearch, TestSiteOpen, TestSiteSave,
              TestSiteRemove, TestSiteGetDescription, TestSiteCreateItem)

capsule_tests = (TestDBCapsule, )
