# coding: utf8

import os
from unittest import TestCase
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
            track_titles = set()
            for album in self.site.search('rest_capsules'):
                for track in album.subitems:
                    if track:
                        compilation.subitems.append(track)
                        track_titles.add(track['title'])
            self.site.save(compilation)
            

            compilation2 = self.site.open('rest_capsules', '"Compilation"')
            self.assertEquals(track_titles, set(track['title']
                for track in compilation2.subitems))
            

