# coding: utf8

import os
import time
from kalamar import Item

# There is some magic at the end of this file :-P

class TestSiteSearch(object):
    
    def test_non_ascii(self):
        """Request with non ascii characters must be handled correctly."""
        request = u'artiste=Birelli Lagrène'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item.properties["artiste"], u'Birelli Lagrène')
        
    def test_without_sugar(self):
        """A request without syntaxic sugar must return the corresponding items."""
        request = u'artiste=Water please/genre=rock'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item.properties["genre"], u'rock')
            self.assertEqual(item.properties["artiste"], u'Water please')
    
    def test_with_sugar(self):
        """A request with syntaxic sugar must guess properties' names from \
configuration."""
        request = u'rock/Water please'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item.properties["genre"], u'rock')
            self.assertEqual(item.properties["artiste"], u'Water please')
    
    def test_all_data(self):
        """An empty search must return the whole collection."""
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
        """Trying to open an item that does not exist must raise the exception \
'ObjectDoesNotExist'."""
        request = u'genre=doesnt_exist'
        self.assertRaises(self.site.ObjectDoesNotExist, self.site.open,
                          self.access_point_name, request)
    
    def test_one_result(self):
        """Trying to open an unique item must return the corresponding item."""
        request = u'genre=rock/artiste=Jesus\'harlem' \
                  u'/album=amen/titre=mechanical blues'
        item = self.site.open(self.access_point_name, request)
        self.assertEqual(item.properties['genre'], u'rock')
        self.assertEqual(item.properties['artiste'], u'Jesus\'harlem')
        self.assertEqual(item.properties['album'], u'amen')
        self.assertEqual(item.properties['titre'], u'mechanical blues')
    
    def test_many_results(self):
        """Try to open many item must raise 'MultiObjectsReturned'."""
        request = u'genre=rock'
        self.assertRaises(self.site.MultipleObjectsReturned, self.site.open,
                          self.access_point_name, request)

class TestSiteSave(object):
    
    def test_new_complete_item(self):
        """Saving a brad new item with all necessary properties set must \
make it available for later search/opening.
        
        This test needs the open method to work."""
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
        # Must not raise any exception
        item2=self.site.open(
            self.access_point_name,
            u'genre=funk/artiste=loopzilla/'
            u'album=demo/titre=many money/piste=2'
        )
        
    def test_two_new_items(self):
        """Two new and differents saved item must both be availables and corrects.
        
        This test needs the open method to work."""
        access_point = self.site.access_points[self.access_point_name]
        
        properties = {'genre': 'funk',
                      'artiste': 'loopzilla',
                      'album': 'demo',
                      'titre': 'many money',
                      'piste': '2'}
        properties2 = {'genre': 'funk',
                       'artiste': 'loopzilla',
                       'album': 'demo',
                       'titre': 'mamma mia',
                       'piste': '3'}
                      
        # Mutagen does not accept to create a VorbisFile
        # without initial content.
        if access_point.config['parser'] == 'audio_vorbis':
            vorbis_file = open(os.path.join(os.path.dirname(__file__),
                                        'data', 'vorbis_sample.ogg'))
            data = vorbis_file.read()
            properties['_content']  = data
            properties2['_content']  = data
            
        item = Item.create_item(access_point, properties)
        item2 = Item.create_item(access_point, properties2)
        
        self.site.save(item)
        self.site.save(item2)
        
        properties.pop('_content')
        properties2.pop('_content')
        request = '/'.join('%s=%s' % prop for prop in properties.items())
        request2 = '/'.join('%s=%s' % prop for prop in properties2.items())
        
        
        item = self.site.open(self.access_point_name, request)
        item2 = self.site.open(self.access_point_name, request2)
        
        self.assertEqual(item.properties['titre'],'many money')
        self.assertEqual(item2.properties['titre'],'mamma mia')
        
        
    def test_new_incomplete_item(self):
        pass # TODO : wtf should kalamar do in this case ?
    
    def test_unmodified_item(self):
        """Saving an unmodified existing item must leave the access point \
unchanged.
        
        This test needs the open method to work."""
        request = u'genre=rock/artiste=Jesus\'harlem/' \
                  u'album=amen/titre=mechanical blues'
        item = self.site.open(self.access_point_name, request)
        self.site.save(item)
        # Must not raise any exception
        self.site.open(self.access_point_name, request)
    
    def test_modified_item(self):
        """Saving a modified existing item must modify it on the access point.
        
        This test needs the open method to work."""
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
        """A removed item must not be available any longer.
        
        This test needs the open method to work."""
        request = u'genre=rock/artiste=Jesus\'harlem/album=amen/titre=cross'
        item = self.site.open(self.access_point_name, request)
        self.site.remove(item)
        self.assertEqual(list(self.site.search(self.access_point_name, request)), [])

