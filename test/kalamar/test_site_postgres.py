# coding: utf8

import warnings
try:
    from pg8000 import dbapi
except ImportError:
    warnings.warn('PostgresSQL access not tested. (Could not import pg8000)')
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
    from test.kalamar import Site

    site = Site(os.path.join(os.path.dirname(__file__), 'data',
                'kalamar_postgres.conf'))

    class TestSite(object):
        
        def setUp(self):
            self.site = site
            
            # assume that all access points connect to the same base
            connection = site.access_points.values()[0].get_connection()[0]
            cursor = connection.cursor()
            try:
                cursor.execute('DELETE FROM textes;')
                cursor.execute(
                    'insert into textes select * from textes_bak;'
                )
                cursor.execute('DELETE FROM morceaux;')
                cursor.execute(
                    'insert into morceaux select * from morceaux_bak;'
                )
                connection.commit()
            finally:
                cursor.close()

    try:
        site.access_points.values()[0].get_connection()
    except Exception, e:
        warnings.warn('PostgresSQL access not tested. ' + unicode(e))
    else:
        # Magic tricks
        for access_point in site.access_points:
            for test in (TestSiteSearch, TestSiteOpen, TestSiteSave,
                         TestSiteRemove):
                cls = type(test.__name__+'_'+access_point, 
                           (TestSite, test, TestCase),
                           {"access_point_name": access_point})
                setattr(sys.modules[__name__], cls.__name__, cls)

