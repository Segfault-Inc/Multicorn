# coding: utf8

import warnings
try:
    import MySQLdb
except ImportError:
    warnings.warn('MySQL access not tested.')
else:
    import os
    import sys
    import shutil
    import tempfile
    import atexit
    from unittest import TestCase
    from test_site_common import TestSiteSearch,\
                                 TestSiteOpen,\
                                 TestSiteSave,\
                                 TestSiteRemove
    from kalamar import Site

    site = Site(os.path.join(os.path.dirname(__file__), 'data',
                'kalamar_mysql.conf'))

    class TestSite(object):
        
        def setUp(self):
            self.site = site
            
        def tearDown(self):
            connection = MySQLdb.connect(user='root', passwd='test',
                                         host='localhost', db='test')
            cursor = connection.cursor()
            cursor.execute('drop table if exists textes;')
            cursor.execute('drop table if exists morceaux;')

            cursor.execute('create table textes like textes_bak;')
            cursor.execute('insert textes select * from textes_bak;')
            cursor.execute('create table morceaux like morceaux_bak;')
            cursor.execute('insert morceaux select * from morceaux_bak;')
    
    # Magic tricks
    for access_point in site.access_points:
        for test in (TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove):
            cls = type(test.__name__+'_'+access_point, (TestSite, test, TestCase),
                       {"access_point_name": access_point})
            setattr(sys.modules[__name__], cls.__name__, cls)
