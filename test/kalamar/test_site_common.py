# coding: utf8

import os
import time
from kalamar import Item

# There is some magic at the end of this file :-P        

class TestSiteSearch(object):
        
    def test_without_sugar(self):
        request = u'genre=jazz/artiste=Birelli Lagrène'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item.properties["genre"], u'jazz')
            self.assertEqual(item.properties["artiste"], u'Birelli Lagrène')
    
    def test_with_sugar(self):
        request = u'jazz/Birelli Lagrène'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item.properties["genre"], u'jazz')
            self.assertEqual(item.properties["artiste"], u'Birelli Lagrène')
    
    def test_all_data(self):
        request = u''
        all_objects = list(self.site.search(self.access_point_name, request))
        genres = set(item.properties["genre"] for item in all_objects)
        artistes = set(item.properties["artiste"] for item in all_objects)
        albums = set(item.properties["album"] for item in all_objects)
        
        self.assertEqual(len(all_objects), 20)
        self.assertEqual(genres, set([u'jazz', u'rock']))
        self.assertEqual(artistes, set([u"Jesus'harlem",
                                        u'Birelli Lagrène',
                                        u'Water please']))
        self.assertEqual(albums, set([u'manouche swing',
                                      u'S.O.B', u'alleluia',
                                      u'amen']))

class TestSiteOpen(object):
    
    def test_no_result(self):
        request = u'genre=doesnt_exist'
        self.assertRaises(self.site.ObjectDoesNotExist, self.site.open,
                          self.access_point_name, request)
    
    def test_one_result(self):
        request = u'genre=rock/artiste=Jesus\'harlem' \
                  u'/album=amen/titre=mechanical blues'
        item = self.site.open(self.access_point_name, request)
        self.assertEqual(item.properties['genre'], u'rock')
        self.assertEqual(item.properties['artiste'], u'Jesus\'harlem')
        self.assertEqual(item.properties['album'], u'amen')
        self.assertEqual(item.properties['titre'], u'mechanical blues')
    
    def test_many_results(self):
        request = u'genre=rock'
        self.assertRaises(self.site.MultipleObjectsReturned, self.site.open,
                          self.access_point_name, request)

class TestSiteSave(object):
    
    def test_new_complete_item(self):
        access_point = self.site.access_points[self.access_point_name]
        
        properties = {'genre': 'funk',
                      'artiste': 'loopzilla',
                      'album': 'demo',
                      'titre': 'many money',
                      'piste': '2'}
                      
        # Mutagen does not accept to create a VorbisFile
        # without initial content.
        if access_point.config['parser'] == 'audio_vorbis':
            vorbis_file = open(os.path.join(os.path.dirname(__file__),
                                        'data', 'vorbis_sample.ogg'))
            data = vorbis_file.read()
            properties['_content']  = data
            
        item = Item.create_item(access_point, properties)
        self.site.save(item)
        item2=self.site.open(
            self.access_point_name,
            u'genre=funk/artiste=loopzilla/'
            u'album=demo/titre=many money/piste=2'
        )
        
    def test_new_incomplete_item(self):
        """Should raise an Exception ?"""
        pass # TODO : wtf should kalamar do in this case ?
    
    def test_unmodified_item(self):
        request = u'genre=rock/artiste=Jesus\'harlem/' \
                  u'album=amen/titre=mechanical blues'
        item = self.site.open(self.access_point_name, request)
        self.site.save(item)
        # Should not raise any exception
        self.site.open(self.access_point_name, request)
    
    def test_modified_item(self):
        request = u'genre=rock/artiste=Jesus\'harlem/' \
                  u'album=alleluia/titre=solomon'
        item = self.site.open(self.access_point_name, request)
        item.properties['genre'] = 'toto'
        item.properties['titre'] = 'tata'
        self.site.save(item)
        self.assertRaises(self.site.ObjectDoesNotExist, self.site.open,
                          self.access_point_name, request)
        # Should not raise any exception
        self.site.open(self.access_point_name,
                       u'genre=toto/artiste=Jesus\'harlem/' \
                       u'album=alleluia/titre=tata')

class TestSiteRemove(object):
    def test_remove(self):
        request = u'genre=rock/artiste=Jesus\'harlem/album=amen/titre=cross'
        item = self.site.open(self.access_point_name, request)
        self.site.remove(item)
        self.assertEqual(list(self.site.search(self.access_point_name, request)), [])

