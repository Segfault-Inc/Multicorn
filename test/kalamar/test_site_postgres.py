# coding: utf8

import warnings
try:
    from pyPgSQL import PgSQL
except ImportError:
    warnings.warn('PostgresSQL access not tested.')
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
                'kalamar_postgres.conf'))

    class TestSite(object):
        
        def setUp(self):
            self.site = site
            
            #get an exclusive connection to drop tables
            for ap in site.access_points.values():
                ap.close_connection()
                
            connection = PgSQL.connect(
                user='kalamar',
                password='kalamar',
                host='localhost',
                database='kalamar'
            )
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute('DROP TABLE IF EXISTS textes;')
                    cursor.execute('DROP TABLE IF EXISTS morceaux;')
                    cursor.execute("""
                        CREATE TABLE TEXTES (
                            LIKE textes_bak
                            INCLUDING DEFAULTS
                            INCLUDING CONSTRAINTS
                            INCLUDING INDEXES );"""
                    )
                    cursor.execute(
                        'insert into textes select * from textes_bak;'
                    )
                    cursor.execute("""
                        create table morceaux (
                            like textes_bak
                            INCLUDING DEFAULTS
                            INCLUDING CONSTRAINTS
                            INCLUDING INDEXES );"""
                    )
                    cursor.execute(
                        'insert into morceaux select * from morceaux_bak;'
                    )
                    connection.commit()
                finally:
                    cursor.close()
            finally:
                connection.close()
            
        def tearDown(self):
            pass
    
    # Magic tricks
    for access_point in site.access_points:
        for test in (TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove):
            cls = type(test.__name__+'_'+access_point, (TestSite, test, TestCase),
                       {"access_point_name": access_point})
            setattr(sys.modules[__name__], cls.__name__, cls)

