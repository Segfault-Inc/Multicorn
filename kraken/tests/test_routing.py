# -*- coding: utf-8 -*-
import os 
from unittest import TestCase

from . import KrakenSiteMixin



class TestRoutedRequests(KrakenSiteMixin, TestCase):
    def test_message(self):
        r = self.client.get('/helloparam/world/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × world!</body></html>\n')

    def test_message_redirect(self):
        r = self.client.get('/helloparam/world')
        self.assertEqual(r.status_code, 301)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.headers['Location'],
                'http://localhost/helloparam/world/')
        self.assert_('redirect' in r.data.lower())
        self.assert_('helloparam/world/' in r.data)

    def test_inexistent(self):
        r = self.client.get('/bibopelula/')
        self.assertEqual(r.status_code, 404)

    def test_methods(self):
        r = self.client.get('/methods/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × GET world!</body></html>\n')
        r = self.client.post('/methods/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × POST world!</body></html>\n')

    def test_hello(self):
        r = self.client.get('/hello/?name=World')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × World!</body></html>\n')

    def test_othertemplate(self):
        r = self.client.get('/another/template/World')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × World!</body></html>\n')

    def test_weirdpath(self):
        r = self.client.get('/World/message')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × World!</body></html>\n')

    def setUp(self):
        super(TestRoutedRequests, self).setUp()
        from . import controllers
        self.test_app.register_controllers(controllers)
        














