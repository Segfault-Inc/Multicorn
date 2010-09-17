
import os.path
import werkzeug

def make_site(secret_key=None):
    # import kraken here so that coverage sees module-level statements
    import kraken
    return kraken.Site(
        site_root=os.path.join(os.path.dirname(__file__), 'site'),
        secret_key=secret_key,
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

