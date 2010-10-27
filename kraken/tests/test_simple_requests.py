# -*- coding: utf-8 -*-
import os
from unittest import TestCase

from . import KrakenSiteMixin


class TestSimpleRequests(KrakenSiteMixin, TestCase):
    def test_notfound(self):
        r = self.client.get('/nonexistent')
        self.assertEqual(r.status_code, 404)

    def test_hidden_notfound(self):
        r2 = self.client.get('/.hidden_but_nonexistent')
        self.assertEqual(r2.status_code, 403)

    def test_hidden_template(self):
        r = self.client.get('/.hidden_template')
        self.assertEqual(r.status_code, 403)

    def test_static_notfound(self):
        r = self.client.get('/logo/inexistent.png')
        self.assertEqual(r.status_code, 404)

    def test_lipsum_notfound(self):
        r = self.client.get('/lorem/ipsum/dolor')
        self.assertEqual(r.status_code, 404)

    def test_index(self):
        r = self.client.get('/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Dyko root</body></html>\n')

    def test_hello(self):
        r = self.client.get('/hello/?name=World')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello × World!</body></html>\n')

    def test_hello_redirect(self):
        r = self.client.get('/hello?name')
        self.assertEqual(r.status_code, 301)
        self.assertEqual(r.headers['Location'], 'http://localhost/hello/?name')
        self.assert_('redirect' in r.data.lower())
        self.assert_('hello/?name' in r.data)


class TestSession(KrakenSiteMixin, TestCase):
    def setUp(self):
        self.site_secret_key = os.urandom(20)
        super(TestSession, self).setUp()

    def test_session(self):
        # get the default value
        r1 = self.client.get("/session/")
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r1.data, "(no value)")

        # set the value
        r2 = self.client.get("/session/?blah")
        self.assertEqual(r2.status_code, 200)

        # get again and check
        r3 = self.client.get("/session/")
        self.assertEqual(r3.status_code, 200)
        self.assertEqual(r3.data, "blah")
