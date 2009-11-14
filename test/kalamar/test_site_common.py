# coding: utf8

import os
import time
from kalamar import Item
from kalamar.storage.dbapi import DBAPIStorage

class MyTest(object):
    """This class is meant to work if co-inherited with unittest.TestCase.
    
    When inheriting, this class must be declared before unittest.TestCase in the
    parents list (i.e. sth like ChildClass(..., MyTest, ...,TestCase, ...))
    
    """
    
    def shortDescription(self):
        """Override unittest.TestCase.shortDescription
        
        Return a custom one-line description of the test in the format
        '(access_point_name) docstring' or None if no docstring.
        
        """
        doc = super(MyTest, self).shortDescription()
        if doc:
            return '(%s) %s' % (self.access_point_name, doc)
        else:
            return None

class TestSiteSearch(MyTest):
    
    def test_non_ascii(self):
        """Request with non ascii characters must be handled correctly."""
        request = u'artiste="Birelli Lagrène"'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item["artiste"], u'Birelli Lagrène')
        
    def test_without_sugar(self):
        """A request without syntaxic sugar must return the corresponding items."""
        request = u'artiste="Water please"/genre="rock"'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item["genre"], u'rock')
            self.assertEqual(item["artiste"], u'Water please')
            
    def test_property_type_string(self):
        # SQLite has no typing
        if not('sqlite' in self.access_point_name or
               'storage' in self.access_point_name):
            request = u'piste="01"'
            
            items = self.site.search(self.access_point_name, request)
            
            if ('fs' in self.access_point_name and
               'classified' in self.access_point_name) or \
               'rest' in self.access_point_name:
                self.assertEqual(len(items), 4)
                for item in items:
                    self.assertEqual(item["piste"], u"01")
            else:
                self.assertEqual(len(items), 0)
        
    
    def test_property_type_integer(self):
        # SQLite has no typing
        if not('sqlite' in self.access_point_name or
               'storage' in self.access_point_name):
            request = u'piste=1'
            
            items = self.site.search(self.access_point_name, request)
            if not('fs' in self.access_point_name and
                   'classified' in self.access_point_name or
                   'rest' in self.access_point_name):
                self.assertEqual(len(items), 4)
            else:
                self.assertEqual(len(items), 0)
                
            for item in items:
                self.assertEqual(item["piste"], 1)
    
    def test_with_sugar(self):
        """Request with syntaxic sugar must guess props names from config."""
        request = u'"rock"/"Water please"'
        for item in self.site.search(self.access_point_name, request):
            self.assertEqual(item["genre"], u'rock')
            self.assertEqual(item["artiste"], u'Water please')
    
    def test_all_data(self):
        """An empty search must return the whole collection."""
        request = u''
        all_objects = list(self.site.search(self.access_point_name, request))
        genres = set(item["genre"] for item in all_objects)
        artistes = set(item["artiste"] for item in all_objects)
        albums = set(item["album"] for item in all_objects)
        
        self.assertEqual(len(all_objects), 20)
        self.assertEqual(genres, set([u'jazz', u'rock']))
        self.assertEqual(artistes, set([u"Jesus'harlem",
                                        u'Birelli Lagrène',
                                        u'Water please']))
        self.assertEqual(albums, set([u'manouche swing',
                                      u'S.O.B', u'alleluia',
                                      u'amen']))
    def test_regexp(self):
        """The regexp operator must behave like re.search in python."""
        request = u'titre ~= "a.*s$"'
        result = list(self.site.search(self.access_point_name, request))
        
        self.assertEqual(len(result), 2)
        for item in result:
            self.assert_('a' in item['titre'])
            self.assertEqual(item['titre'][-1], 's')

class TestSiteOpen(MyTest):
    
    def test_no_result(self):
        """Opening an non-existing item must raise ``ObjectDoesNotExist``."""
        request = u'genre="doesnt_exist"'
        self.assertRaises(self.site.ObjectDoesNotExist, self.site.open,
                          self.access_point_name, request)
    
    def test_one_result(self):
        """Trying to open an unique item must return the corresponding item."""
        request = u'genre="rock"/artiste="Jesus\'harlem"' \
                  u'/album="amen"/titre="mechanical blues"'
        item = self.site.open(self.access_point_name, request)
        self.assertEqual(item['genre'], u'rock')
        self.assertEqual(item['artiste'], u'Jesus\'harlem')
        self.assertEqual(item['album'], u'amen')
        self.assertEqual(item['titre'], u'mechanical blues')
    
    def test_many_results(self):
        """Try to open many item must raise 'MultiObjectsReturned'."""
        request = u'genre="rock"'
        self.assertRaises(self.site.MultipleObjectsReturned, self.site.open,
                          self.access_point_name, request)

class TestSiteSave(MyTest):
    
    def test_new_complete_item(self):
        """New item saved with necessary properties must make it available.
        
        This test needs the open method to work.

        """
        access_point = self.site.access_points[self.access_point_name]
        
        properties = {'genre': 'funk',
                      'artiste': 'loopzilla',
                      'album': 'demo',
                      'titre': 'many money'}
                      
        if not('fs' in self.access_point_name and
               'classified' in self.access_point_name or
               'rest' in self.access_point_name):
            properties['piste'] = 2
        else:
            properties['piste'] = '2'
                      
        item = Item.create_item(access_point, properties)

        # Mutagen does not accept to create a VorbisFile
        # without initial content.
        if access_point.parser_name == 'audio_vorbis':
            vorbis_file = open(os.path.join(os.path.dirname(__file__),
                                            'data', 'vorbis_sample.ogg'))
            item._raw_content = vorbis_file.read()
            vorbis_file.close()
            item._loaded = True # prevent _parse_data from overwriting
                                # the properties we just set
            
        self.site.save(item)
        
        if not('fs' in self.access_point_name and
               'classified' in self.access_point_name or
               'rest' in self.access_point_name):
            request = u'''genre="funk"/artiste="loopzilla"/
                          album="demo"/titre="many money"/piste=2'''
        else:
            request = u'''genre="funk"/artiste="loopzilla"/
                          album="demo"/titre="many money"/piste="2"'''
        
        # Must not raise any exception
        item2 = self.site.open(self.access_point_name, request)
        
    def test_two_new_items(self):
        """Two new and different saved items must both be available and correct.
        
        This test needs the open method to work."""
        access_point = self.site.access_points[self.access_point_name]
        
        properties = {'genre': 'funk',
                      'artiste': 'loopzilla',
                      'album': 'demo',
                      'titre': 'many money'}
        properties2 = {'genre': 'funk',
                       'artiste': 'loopzilla',
                       'album': 'demo',
                       'titre': 'mamma mia'}
        
        if not('fs' in self.access_point_name and
                'classified' in self.access_point_name or
                'rest' in self.access_point_name):
            properties['piste'] = 2
            properties2['piste'] = 3
        else:
            properties['piste'] = '2'
            properties2['piste'] = '3'
                      
        item = Item.create_item(access_point, properties)
        item2 = Item.create_item(access_point, properties2)
        
        # Mutagen does not accept to create a VorbisFile
        # without initial content.
        if access_point.parser_name == 'audio_vorbis':
            vorbis_file = open(os.path.join(os.path.dirname(__file__),
                                            'data', 'vorbis_sample.ogg'))
            item._raw_content = item2._raw_content = vorbis_file.read()
            vorbis_file.close()
            
        self.site.save(item)
        self.site.save(item2)
        
        properties.pop('_content')
        properties2.pop('_content')
        
        if not('fs' in self.access_point_name and
                'classified' in self.access_point_name or
                'rest' in self.access_point_name):
            request = '/'.join('%s="%s"' % prop for prop in properties.items() if prop[0] != 'piste')
            request2 = '/'.join('%s="%s"' % prop for prop in properties2.items() if prop[0] != 'piste')
            request += '/piste=%i' % properties['piste']
            request2 += '/piste=%i' % properties2['piste']
        else:
            request = '/'.join('%s="%s"' % prop for prop in properties.items())
            request2 = '/'.join('%s="%s"' % prop for prop in properties2.items())

        item = self.site.open(self.access_point_name, request)
        item2 = self.site.open(self.access_point_name, request2)
        
        self.assertEqual(item['titre'], 'many money')
        self.assertEqual(item2['titre'], 'mamma mia')
        
    def test_new_incomplete_item(self):
        pass # TODO : wtf should kalamar do in this case ?
    
    def test_unmodified_item(self):
        """Saving an unmodified existing item must not change the access point.
        
        This test needs the open method to work.

        """
        request = u'genre="rock"/artiste="Jesus\'harlem"/' \
                  u'album="amen"/titre="mechanical blues"'
        item = self.site.open(self.access_point_name, request)
        self.assertEqual(item.storage_modified, False)
        self.assertEqual(item.parser_modified, False)
        self.site.save(item)
        # Must not raise any exception
        self.site.open(self.access_point_name, request)
    
    def test_modified_item(self):
        """Saving a modified existing item must modify it on the access point.
        
        This test needs the open method to work.

        """
        
        # !! This test needs the "fs_text_mixed" access point defined in
        # $test$/kalamar/data/kalamar_fs_and_sqlite.conf
        request = u'genre="rock"/album="alleluia"/titre="solomon"'
        item = self.site.open(self.access_point_name, request)
        item['artiste'] = 'toto'
        item['titre'] = 'tata'
        self.site.save(item)
        self.assertRaises(self.site.ObjectDoesNotExist, self.site.open,
                          self.access_point_name, request)
        # Should not raise any exception
        self.site.open(self.access_point_name,
                       u'genre="rock"/artiste="toto"/'
                       u'album="alleluia"/titre="tata"')

class TestSiteRemove(MyTest):
    def test_remove(self):
        """A removed item must not be available any longer.
        
        This test needs the open method to work.

        """
        request = u'genre="rock"/artiste="Jesus\'harlem"/album="amen"/titre="cross"'
        item = self.site.open(self.access_point_name, request)
        self.site.remove(item)
        self.assertEqual(list(self.site.search(self.access_point_name, request)), [])

class TestSiteGetDescription(MyTest):
    def test_get_description(self):
        """TODO
        """
        description = self.site.get_description(self.access_point_name)
        self.assertEqual(description, self.site.access_points[self.access_point_name].property_names)

class TestSiteCreateItem(MyTest):
    def test_remove(self):
        """TODO
        
        This test needs the open method to work.

        """
        # Vorbis parser is read only
        if self.site.access_points[self.access_point_name].parser_name != 'audio_vorbis':
            request = u'genre="rock"/artiste="Jesus\'harlem"/album="love is love"/titre="GreatGreatGod"'
            properties = {'genre': 'rock', 'artiste': "Jesus'harlem",
                          'album': 'love is love', 'titre': 'GreatGreatGod'}
            item_create = self.site.create_item(self.access_point_name, properties)
            self.site.save(item_create)
            item_open = self.site.open(self.access_point_name, request)
            for tag in ('genre', 'artiste', 'album', 'titre', 'unknown'):
                self.assertEqual(item_create[tag], item_open[tag])
