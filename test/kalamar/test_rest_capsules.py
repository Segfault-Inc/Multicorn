# coding: utf8

import os
from unittest import TestCase
from kalamar import Site
from test.kalamar.test_site_fs_and_sqlite import TestData

import warnings
try:
    import docutils
except ImportError:
    warnings.warn('ReST parser not tested. (Could not import docutils)',
                  stacklevel=2)
else:

    class TestSite(TestCase):
        
        def setUp(self):
            self.temp_dir = TestData.get_temp_dir()
            self.site = Site(os.path.join(self.temp_dir, 'rest_capsules.conf'))
        
        def test_consistency(self):
            # all albums
            for album in  self.site.search('rest_capsules'):
                for track in album.subitems:
                    if track:
                        # check that this tracks belongs to this album
                        for prop in ('genre', 'artist', 'album'):
                            self.assertEquals(album.properties[prop],
                                              track.properties[prop])
                    else:
                        self.assertEquals(track.filename, 'MISSING.rst')
                        self.assertEquals(repr(track),
                                          "<MissingItem u'MISSING.rst'>")
        
        def test_album_length(self):
            album_length = {}
            for album in  self.site.search('rest_capsules'):
                album_length[album.properties['album']] = sum(1
                    for track in album.subitems if track)
            self.assertEquals(album_length, {u'manouche swing': 3, u'amen': 8,
                                             u'alleluia': 7, u'S.O.B': 2})

        def test_remove_last(self):
            for album in  self.site.search('rest_capsules'):
                # list track titles
                tracks = [track.properties['title']
                          for track in album.subitems if track]
                          
                # remove the last non-missing one
                while not album.subitems.pop():
                    pass
                self.assert_(album.subitems.modified)
                
                # save to ReST file and load back
                self.site.save(album)
                request = '/'.join(prop + '="' + album.properties[prop] + '"'
                                   for prop in ('genre', 'artist', 'album'))
                album2 = self.site.open('rest_capsules', request)
                
                tracks2 = [track.properties['title']
                           for track in album2.subitems if track]
                # verify that what we get back is what we saved
                self.assertEquals(tracks2, tracks[:-1])


