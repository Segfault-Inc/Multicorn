# coding: utf8

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

class TestData(object):
    _original_data = os.path.join(os.path.dirname(__file__), 'data')
    _dirname = None
    
    @classmethod
    def get_temp_dir(cls):
        if cls._dirname is None:
            atexit.register(cls.cleanup)
            cls._dirname = tempfile.mkdtemp()
        cls.mini_rsync(cls._original_data, cls._dirname)
        return cls._dirname
    
    @classmethod
    def mini_rsync(cls, src, dst):
        names = os.listdir(src)
        
        # remove files and dirs that aren’t in src
        for name in os.listdir(dst):
            dstname = os.path.join(dst, name)
            if name not in names:
                if os.path.isdir(dstname):
                    shutil.rmtree(dstname)
                else:
                    os.remove(dstname)

        for name in names:
            srcname = os.path.join(src, name)
            dstname = os.path.join(dst, name)
            if os.path.isdir(srcname):
                if not os.path.isdir(dstname):
                    os.makedirs(dstname)
                cls.mini_rsync(srcname, dstname)
                shutil.copystat(srcname, dstname)
            else:
                try:
                    dststat = os.stat(dstname)
                except OSError:
                    pass
                else:
                    srcstat = os.stat(srcname)
                    if (srcstat.st_size, srcstat.st_mtime) == \
                       (dststat.st_size, dststat.st_mtime):
                        # the file is probably the same in src and dst: skip it
                        continue
                shutil.copy2(srcname, dstname)
        
    @classmethod
    def cleanup(cls):
        if cls._dirname is not None:
           shutil.rmtree(cls._dirname)
        

class TestSite(object):
    
    def setUp(self):
        self.temp_dir = TestData.get_temp_dir()
        self.site = Site(os.path.join(self.temp_dir, 'kalamar_fs_and_sqlite.conf'))
    
    def tearDown(self):
        pass


site = Site(os.path.join(os.path.dirname(__file__), 'data',
            'kalamar_fs_and_sqlite.conf'))

for access_point in site.access_points:
    for test in (TestSiteSearch, TestSiteOpen, TestSiteSave, TestSiteRemove):
        cls = type(test.__name__+'_'+access_point, (TestSite, test, TestCase),
                   {"access_point_name": access_point})
        setattr(sys.modules[__name__], cls.__name__, cls)
