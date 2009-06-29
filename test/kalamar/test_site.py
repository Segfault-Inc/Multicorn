
import os
import sys
sys.path.append(os.path.normpath(os.path.realpath(__file__) + '/../../../src/'))
from kalamar import Site

class TestSite():
    
    def setUp(self):
        self.site = Site(os.path.normpath(os.path.realpath(__file__)
                         + '/../data/kalamar.conf'))

class TestSiteSearch(TestSite):
    
    def setUp(self):
        super(TestSite, self).setUp()
        self.test_result = []
        
    def test_without_sugar(self):
        request = 'genre=jazz/artiste=Birelli Lagrène'
        self.assertEqual(self.site.search(request), self.test_result)
        
    def test_with_sugar(self):
        request = 'jazz/Birelli Lagrène'
        self.assertEqual(self.site.search(request), self.test_result)

class TestSiteOpen(TestSite):
    pass

class TestSiteSave(TestSite):
    pass

class TestSiteRemove(TestSite):
    pass


