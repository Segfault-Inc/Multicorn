# coding: utf8

import os
from unittest2 import TestCase
from test.kalamar import Site
from test.kalamar.test_site_fs_and_sqlite import TestData

import warnings
try:
    import docutils
except ImportError:
    warnings.warn('ReST parser not tested. (Could not import docutils)',
                  ImportWarning,
                  stacklevel=2)
else:
    class TestCapsules(TestCase):
        def setUp(self):
            self.temp_dir = TestData.get_temp_dir()
            self.site = Site(os.path.join(self.temp_dir, 'rest_capsules.conf'))
        
        def test_consistency(self):
            # all albums
            for album in self.site.search('rest_capsules'):
                for track in album.subitems:
                    if track:
                        # check that this tracks belongs to this album
                        for prop in ('genre', 'artist', 'album'):
                            self.assertEquals(album[prop],
                                              track[prop])
                    else:
                        self.assertEquals(track.filename, 'MISSING.rst')
                        self.assertEquals(repr(track),
                                          "<MissingItem u'MISSING.rst'>")
        
        def test_album_length(self):
            album_length = {}
            for album in self.site.search('rest_capsules'):
                album_length[album['album']] = sum(1
                    for track in album.subitems if track)
            self.assertEquals(album_length, {u'manouche swing': 3, u'amen': 8,
                                             u'alleluia': 7, u'S.O.B': 2})

        def test_album_length_with_missing(self):
            album_length = {}
            for album in self.site.search('rest_capsules'):
                album_length[album['album']] = len(album)
            self.assertEquals(album_length, {u'manouche swing': 4, u'amen': 8,
                                             u'alleluia': 7, u'S.O.B': 2})

        def test_contains(self):
            item = self.site.open('fs_text_messed_up',
                                  {'album': 'alleluia', 'title': 'juda'})
            album = self.site.open('rest_capsules', {'album': 'alleluia'})
            self.assert_(item in album)

        def test_remove_last(self):
            for album in self.site.search('rest_capsules'):
                # list track titles
                tracks = [track['title']
                          for track in album.subitems if track]
                          
                # remove the last non-missing one
                while not album.subitems.pop():
                    pass
                self.assert_(album.subitems.modified)
                
                # save to ReST file and load back
                self.site.save(album)
                request = '/'.join(prop + '="' + album[prop] + '"'
                                   for prop in ('genre', 'artist', 'album'))
                album2 = self.site.open('rest_capsules', request)
                
                tracks2 = [track['title']
                           for track in album2.subitems if track]
                # verify that what we get back is what we saved
                self.assertEquals(tracks2, tracks[:-1])
        
        def test_create(self):
            """
            create a new capsule from stratch, save it, load it back
            and check we get it as expected
            """
            compilation = self.site.create_item('rest_capsules', 
                                                dict(album='Compilation'))
            track_titles = []
            for album in self.site.search('rest_capsules'):
                for track in album.subitems:
                    if track:
                        compilation.subitems.append(track)
                        track_titles.append(track['title'])
            self.site.save(compilation)

            compilation2 = self.site.open('rest_capsules', '"Compilation"')
            self.assertEquals(track_titles, [track['title']
                for track in compilation2.subitems])
            
        def test_set_subitems(self):
            compilation = self.site.create_item('rest_capsules', 
                                                dict(album='Compilation'))
            track_titles = []
            for album in self.site.search('rest_capsules'):
                for track in album:
                    if track:
                        track_titles.append(track)
            compilation.subitems = track_titles
            self.site.save(compilation)

            compilation2 = self.site.open('rest_capsules', ['Compilation'])
            self.assertEquals(track_titles, [item for item in compilation2])
