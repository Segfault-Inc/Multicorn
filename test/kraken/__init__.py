
import os.path
import werkzeug
from kalamar.config import baseparser


def make_site(secret_key=None):
    # import kraken here so that coverage sees module-level statements
    import kraken
    return kraken.Site(
        site_root=os.path.join(os.path.dirname(__file__), 'site'),
        kalamar_conf=baseparser.parse(os.path.join(os.path.dirname(__file__), '..',
                                  'kalamar', 'data',
                                  'kalamar_fs_and_sqlite.conf')),
        secret_key=secret_key,
        fail_on_inexistent_parser=False,
    )

class KrakenSiteMixin(object):
    test_app = None
    def setUp(self):
        """
        Create a ``Client`` that simulates HTTP requests
        See http://werkzeug.pocoo.org/documentation/0.5/test.html
        """
        if self.__class__.test_app is None:
            self.__class__.test_app = make_site(
                secret_key=getattr(self, 'site_secret_key', None)
            )
        
        self.client = werkzeug.Client(self.test_app, werkzeug.BaseResponse)

