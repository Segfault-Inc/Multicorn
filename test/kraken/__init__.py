
import os.path
from unittest import TestCase
import werkzeug


def make_site():
    # import kraken here so that coverage sees module-level statements
    import kraken
    return kraken.Site(
        site_root=os.path.join(os.path.dirname(__file__), 'site'),
        kalamar_conf=os.path.join(os.path.dirname(__file__), '..',
                                  'kalamar', 'data',
                                  'kalamar_fs_and_sqlite.conf'),
    )

class KrakenSiteMixin(object):
    def setUp(self):
        """
        Create a ``Client`` that simulates HTTP requests
        See http://werkzeug.pocoo.org/documentation/0.5/test.html
        """
        self.test_app = make_site()
        
        self.client = werkzeug.Client(self.test_app, werkzeug.BaseResponse)

class TestHello(KrakenSiteMixin, TestCase):
    def test_index(self):
        r = self.client.get('/')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<!DOCTYPE html PUBLIC '
                         '"-//W3C//DTD HTML 4.01//EN" '
                         '"http://www.w3.org/TR/html4/strict.dtd">\n'
                         '<html><body>Dyko root</body></html>')
    def test_hello(self):
        r = self.client.get('/hello')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers['Content-Type'], 'text/html; charset=utf-8')
        self.assertEqual(r.data, '<html><body>Hello, World!</body></html>')

