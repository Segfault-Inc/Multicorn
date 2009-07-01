# coding: utf8

import os
import sys
import shutil
from unittest import TestCase
from kalamar import Site
from kalamar import Item

# There is some magic at the end of this file :-P

class TestSite():
    
    def setUp(self):
        pass
    #def tearDown(self):
    #    shutil.rmtree(os.path.normpath(os.path.join(os.path.realpath(__file__),
    #                                                '..','data_sandbox')))

class TestSiteSearch(TestSite):
        
    def test_without_sugar(self):
        request = u'genre=jazz/artiste=Birelli Lagrène'
        for item in self.site.search(self.access_point, request):
            self.assertEqual(item.properties["genre"], u'jazz')
            self.assertEqual(item.properties["artiste"], u'Birelli Lagrène')
    
    def test_with_sugar(self):
        request = u'jazz/Birelli Lagrène'
        for item in self.site.search(self.access_point, request):
            self.assertEqual(item.properties["genre"], u'jazz')
            self.assertEqual(item.properties["artiste"], u'Birelli Lagrène')
    
    def test_all_data(self):
        request = u''
        all_objects = list(self.site.search(self.access_point, request))
        genres = set(item.properties["genre"] for item in all_objects)
        artistes = set(item.properties["artiste"] for item in all_objects)
        albums = set(item.properties["album"] for item in all_objects)
        
        self.assertEqual(len(all_objects), 20)
        self.assertEqual(genres, set(('jazz', 'rock')))

class TestSiteOpen(TestSite):
    
    def test_no_result(self):
        request = u'genre=doesnt_exist'
        self.assertRaises(self.site.ObjectDoesNotExist, self.site.open,
                          self.access_point, request)
    
    def test_one_result(self):
        request = u'genre=rock/artiste=Jesus\'harlem/album=amen/titre=mechanical blues'
        item = self.site.open(self.access_point, request)
        self.assertEqual(item.properties['genre'], u'rock')
        self.assertEqual(item.properties['artiste'], u'Jesus\'harlem')
        self.assertEqual(item.properties['album'], u'amen')
        self.assertEqual(item.properties['titre'], u'mechanical blues')
    
    def test_many_results(self):
        request = u'genre=rock'
        self.assertRaises(self.site.MultipleObjectsReturned, self.site.open,
                          access_point, request)

class TestSiteSave(TestSite):
    #save new item
    #save unmodified item
    #save modified item
    pass # TODO

class TestSiteRemove(TestSite):
    pass
    #def test_remove(self):
        #request = u'genre=rock/artiste=Jesus\'harlem/album=amen/titre=cross'
        #item = self.site.open(self.access_point, request)
        #self.site.remove(item)
        #self.assertEqual(self.site.search(self.access_point, request), [])

site = Site(os.path.normpath(os.path.join(os.path.realpath(__file__),
                                          '../data/kalamar.conf')))
for access_point in site.access_points:
    for test in (TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove):
        cls = type(test.__name__+'_'+access_point, (test, TestCase),
                   {"access_point": access_point, "site": site})
        setattr(sys.modules[__name__], cls.__name__, cls)

