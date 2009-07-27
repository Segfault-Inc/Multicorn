# coding: utf8

from unittest import TestCase

from test.kraken import KrakenSiteMixin

class TestKrakenWithKalamar(KrakenSiteMixin, TestCase):
    def test_index(self):
        r = self.client.get('/kalamar/artiste=Birelli Lagrène/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/plain; charset=utf-8')
        self.assertEqual(r.data,
            "{u'genre': u'jazz', u'artiste': u'Birelli Lagr\\xe8ne', "
            "u'album': u'manouche swing', u'titre': u'a remark you made'}\n"
            "{u'genre': u'jazz', u'artiste': u'Birelli Lagr\\xe8ne', "
            "u'album': u'manouche swing', u'titre': u'swing it !'}\n"
            "{u'genre': u'jazz', u'artiste': u'Birelli Lagr\\xe8ne', "
            "u'album': u'manouche swing', u'titre': u'tralalaitou'}"
        )

