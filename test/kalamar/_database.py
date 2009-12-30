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

from pprint import pprint

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
            cursor.execute('DELETE FROM textes_onetomany;')
            cursor.execute('INSERT INTO textes_onetomany SELECT * FROM textes_onetomany_bak;')
            cursor.execute('DELETE FROM generic_capsule_link;')
            cursor.execute('INSERT INTO generic_capsule_link SELECT * FROM generic_capsule_link_bak;')
            connection.commit()
        finally:
            cursor.close()

class TestDBCapsule(MyTest):
    
    def test_ordered(self):
        """
        Test if items in a capsule are correctly ordered
        """
        compil=self.site.open(self.access_point_name, 'title="Great BestOf"')
        self.assertEquals(
            ['alleluia', 'a remark you made', 'tralalaitou'],
            [item['title'] for item in compil.subitems]
        )
    
    def test_reorder(self):
        """
        Change order and check if the new order is saved correctly
        """
        compil=self.site.open(self.access_point_name, 'title="Great BestOf"')
        
        last = compil.subitems.pop(-1)
        compil.subitems.insert(0, last)
        self.site.save(compil)
        
        compil=self.site.open(self.access_point_name, 'title="Great BestOf"')
        self.assertEquals(
            ['tralalaitou', 'alleluia', 'a remark you made'],
            [item['title'] for item in compil.subitems]
        )
    
    def test_remove(self):
        """
        Remove a subitem from the capsule, save the capsule and check
        """
        compil=self.site.open(self.access_point_name, 'title="Great BestOf"')
        
        compil.subitems.pop(0)
        self.site.save(compil)
        
        compil=self.site.open(self.access_point_name, 'title="Great BestOf"')
        self.assertEquals(
            ['a remark you made', 'tralalaitou'],
            [item['title'] for item in compil.subitems]
        )
        
    
    def test_bestof_length(self):
        bestof_length = {}
        for bestof in self.site.search(self.access_point_name):
            bestof_length[bestof['title']] = len([
                track for track in bestof.subitems if track
            ])
        self.assertEquals(bestof_length, {u'Great BestOf': 3,
                                          u'Best of the Lord': 2})

    def test_create(self):
        """
        create a new capsule from stratch, save it, load it back
        and check we get it as expected
        """

        # if you get this error:
        # [..., u'tralalaitou', u'tralalaitou', ...] != [..., u'tralalaitou', ...]
        # Make sure the SQL middle table has no UNIQUE constraint on 
        # (capsule_id, item_id)
        # ie we want the same item more than once in a given capsule
        compilation = self.site.create_item(self.access_point_name,
                                            {'title': 'Compilation'})

        # Save and load back so that we get the database-generated ID
        self.site.save(compilation)
        compilation = self.site.open(self.access_point_name, 'title="Compilation"')
        
        # Make a capsule with every track
        track_titles = []
        track_association_properties = []
        for track in self.site.search('textes'):
            compilation.subitems.append(track)
            track_titles.append(track['title'])
            track_association_properties.append(track.association_properties)
        self.site.save(compilation)

        compilation2 = self.site.open(self.access_point_name, 'title="Compilation"')
        
        self.assertEquals(track_titles, [track['title']
            for track in compilation2.subitems])
        self.assertEquals(track_association_properties, [track.association_properties
            for track in compilation2.subitems])
            


site_tests = (TestSiteSearch, TestSiteOpen, TestSiteSave,
              TestSiteRemove, TestSiteGetDescription, TestSiteCreateItem)

capsule_tests = (TestDBCapsule, )
