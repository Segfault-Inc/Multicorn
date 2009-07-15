
import os.path
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

