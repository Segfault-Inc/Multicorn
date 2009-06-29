# coding: utf8

import os
import sys
from unittest import TestCase
from kalamar import Site
from kalamar import Item

class TestSite(TestCase):
    
    def setUp(self):
        self.site = Site(os.path.normpath(os.join(os.path.realpath(__file__),
                                          '../data/kalamar.conf')))

class TestSiteSearch(TestSite):
        
    def test_without_sugar(self):
        request = 'genre=jazz/artiste=Birelli Lagrène'
        for access_point in self.site.access_points:
            for item in self.site.search(access_point, request):
                self.assertEqual((item.properties["genre"],
                                  item.properties["artiste"]),
                                 ('jazz', 'Birelli Lagrène')
                                )
    
    def test_with_sugar(self):
        request = 'jazz/Birelli Lagrène'
        for access_point in self.site.access_points:
            for item in self.site.search(access_point, request):
                self.assertEqual((item.properties["genre"],
                                  item.properties["artiste"]),
                                 ('jazz', 'Birelli Lagrène')
                                )

class TestSiteOpen(TestSite):
    
    def test_no_result(self):
        request = 'genre=doesnt_exist'
        for access_point in self.site.access_points:
            self.assertRaises(self.site.ObjectDoesNotExist,
                              self.site.open, access_point, request)
    
    def test_one_result(self):
        request = 'genre=rock/artiste=Jesus\'harlem/album=amen/title=mechanical blues'
        for access_point in self.site.access_points:
            item = self.site.open(access_point, request)
            self.assertEqual(item.properties['genre'], 'rock')
            self.assertEqual(item.properties['artiste'], 'Jesus\'harlem')
            self.assertEqual(item.properties['album'], 'amen')
            self.assertEqual(item.properties['title'], 'mechanical blues')
    
    def test_many_result(self):
        request = 'genre=rock'
        for access_point in self.site.access_points:
            self.assertRaises(self.site.MultipleObjectsReturned,
                              self.site.open, access_point, request)

class TestSiteSave(TestSite):
    #save new item
    #save unmodified item
    #save modified item
    pass # TODO

class TestSiteRemove(TestSite):
    
    def test_remove(self):
        request = 'genre=rock/artiste=Jesus\'harlem/album=amen/title=cross'
        self.site.remove(access_point, request)
        self.assertEqual(self.site.search(access_point, request), [])

