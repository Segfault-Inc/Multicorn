import os 
from unittest import TestCase

from . import KrakenApplicationMixin
from kraken.routing import expose


@expose("/helloparam/<string:message>/")
def helloparam(request, message, **kwargs):
    return {"message" : message}

@expose("/methods/",methods=("GET",))
def getmethod(request, **kwargs):
    return {"message": "GET world"}

@expose("/methods/",methods=("POST",))
def postmethod(request, **kwargs):
    return {"message": "POST world"}

@expose()
def hello(request, **kwargs):
    return {'request' : request}

@expose("/another/template/<string:message>", template="helloparam")
def anothertemplate(request, message, **kwargs):
    return {'message': message}

@expose("/<string:hello>/message")
def weirdpath(request, hello, **kwargs):
    return {'message': hello} 

class TestSimpleRequests(KrakenApplicationMixin, TestCase):
    def test_message(self):
        r = self.client.get('/helloparam/world/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello, world!</body></html>\n')

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
        self.assertEqual(r.data, '<html><body>Hello, GET world!</body></html>\n')
        r = self.client.post('/methods/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello, POST world!</body></html>\n')

    def test_hello(self):
        r = self.client.get('/hello/?name=World')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello, World!</body></html>\n')

    def test_othertemplate(self):
        r = self.client.get('/another/template/World')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello, World!</body></html>\n')

    def test_weirdpath(self):
        r = self.client.get('/World/message')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello, World!</body></html>\n')

    def test_logo(self):
        r = self.client.get('/static/dyko.png')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'image/png')
        # Maybe check the actual content here instead of just the length ?
        self.assertEqual(len(r.data), 12677)

    def test_logo_etag(self):
        # assume that self.test_logo() passed
        initial_response = self.client.get('/static/dyko.png')
        
        response = self.client.get('/static/dyko.png', headers=[
            ('If-None-Match', initial_response.headers['ETag']),
        ])
        self.assertEqual(response.status_code, 304)
        self.assertEqual(response.data, '')

    def test_logo_last_modified(self):
        # assume that self.test_logo() passed
        initial_response = self.client.get('/static/dyko.png')
        
        response = self.client.get('/static/dyko.png', headers=[
            ('If-Modified-Since', initial_response.headers['Last-Modified']),
        ])
        self.assertEqual(response.status_code, 304)
        self.assertEqual(response.data, '')

    def test_logo_etag_and_last_modified(self):
        # assume that self.test_logo() passed
        initial_response = self.client.get('/static/dyko.png')
        
        response = self.client.get('/static/dyko.png', headers=[
            ('If-None-Match', initial_response.headers['ETag']),
            ('If-Modified-Since', initial_response.headers['Last-Modified']),
        ])
        self.assertEqual(response.status_code, 304)
        self.assertEqual(response.data, '')












