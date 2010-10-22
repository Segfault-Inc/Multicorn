import os.path
import werkzeug


def make_site(secret_key=None):
    # import kraken here so that coverage sees module-level statements
    import kraken
    root = os.path.join(os.path.dirname(__file__), 'site')
    return kraken.Site(root,
            root, 
            secret_key=secret_key,
            static_path=os.path.join(root, '__logo'),
            static_url="/static/")





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
    def tearDown(self):
        from kraken import site
        self.__class__.test_app = None

        site.url_map = werkzeug.routing.Map()


