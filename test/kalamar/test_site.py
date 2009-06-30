# coding: utf8

import os
import sys
from unittest import TestCase
from kalamar import Site
from kalamar import Item

class TestSite(TestCase):
    
    def setUp(self):
        self.site = Site(os.path.normpath(os.path.join(os.path.realpath(__file__),
                                                       '../data/kalamar.conf')))

class TestSiteSearch(TestSite):
        
    def test_without_sugar(self):
        request = u'genre=jazz/artiste=Birelli Lagrène'
        for access_point in self.site.access_points:
            for item in self.site.search(access_point, request):
                self.assertEqual((item.properties["genre"],
                                  item.properties["artiste"]),
                                 ('jazz', 'Birelli Lagrène')
                                )
    
    def test_with_sugar(self):
        request = u'jazz/Birelli Lagrène'
        for access_point in self.site.access_points:
            for item in self.site.search(access_point, request):
                self.assertEqual((item.properties["genre"],
                                  item.properties["artiste"]),
                                 (u'jazz', u'Birelli Lagrène')
                                )

class TestSiteOpen(TestSite):
    
    def test_no_result(self):
        request = u'genre=doesnt_exist'
        for access_point in self.site.access_points:
            self.assertRaises(self.site.ObjectDoesNotExist,
                              self.site.open, access_point, request)
    
    def test_one_result(self):
        request = u'genre=rock/artiste=Jesus\'harlem/album=amen/title=mechanical blues'
        for access_point in self.site.access_points:
            item = self.site.open(access_point, request)
            self.assertEqual(item.properties['genre'], u'rock')
            self.assertEqual(item.properties['artiste'], u'Jesus\'harlem')
            self.assertEqual(item.properties['album'], u'amen')
            self.assertEqual(item.properties['title'], u'mechanical blues')
    
    def test_many_result(self):
        request = u'genre=rock'
        for access_point in self.site.access_points:
            print access_point
            self.assertRaises(self.site.MultipleObjectsReturned,
                              self.site.open, access_point, request)

class TestSiteSave(TestSite):
    #save new item
    #save unmodified item
    #save modified item
    pass # TODO

class TestSiteRemove(TestSite):
    
    def test_remove(self):
        request = u'genre=rock/artiste=Jesus\'harlem/album=amen/title=cross'
        for access_point in self.site.access_points:
            self.site.remove(access_point, request)
            self.assertEqual(self.site.search(access_point, request), [])

