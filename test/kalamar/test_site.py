# coding: utf8

import os
import sys
import shutil
import tempfile
from unittest import TestCase

from kalamar import Site
from kalamar import Item

# There is some magic at the end of this file :-P

class TestSite(object):
    
    def setUp(self):
        src = os.path.join(os.path.dirname(__file__), 'data')
        self.temp_dir = tempfile.mkdtemp()
        shutil.copytree(src, os.path.join(self.temp_dir, 'data'))
        
        self.site = Site(os.path.join(self.temp_dir, 'data', 'kalamar.conf'))
    
    def tearDown(self):
        for dirpath, dirnames, filenames in os.walk(self.temp_dir, topdown=False):
            for f in filenames:
                os.remove(os.path.join(dirpath, f))
            for d in dirnames:
                os.rmdir(os.path.join(dirpath, d))
        os.rmdir(self.temp_dir)

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
        self.assertEqual(genres, set([u'jazz', u'rock']))
        self.assertEqual(artistes, set([u"Jesus'harlem", u'Birelli Lagrène',
                                        u'Water please']))
        self.assertEqual(albums, set([u'manouche swing', u'S.O.B', u'alleluia',
                                      u'amen']))

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
    
    def test_new_item(self):
        pass #TODO
    
    def test_unmodified_item(self):
        pass #TODO
    
    def test_modified_item(self):
        pass #TODO

class TestSiteRemove(TestSite):
    def test_remove(self):
        request = u'genre=rock/artiste=Jesus\'harlem/album=amen/titre=cross'
        item = self.site.open(self.access_point, request)
        self.site.remove(item)
        self.assertEqual(list(self.site.search(self.access_point, request)), [])

# Magic tricks
site = Site(os.path.join(os.path.dirname(__file__), 'data', 'kalamar.conf'))

for access_point in site.access_points:
    for test in (TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove):
        cls = type(test.__name__+'_'+access_point, (test, TestCase),
                   {"access_point": access_point})
        setattr(sys.modules[__name__], cls.__name__, cls)

