# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Koral library.  If not, see <http://www.gnu.org/licenses/>.

import os
import glob
from kalamar.storage.base import AccessPoint

def file_opener(filename):
    def _opener():
        return open(filename, 'rb')
    return _opener
        

class FileSystemStorage(AccessPoint):
    """
    Store items in files
    """
    
    protocol = 'file'
    
    def __init__(self, **config):
        """
        >>> ap = AccessPoint.from_url(basedir='/foo', url='file://bar')
        >>> assert isinstance(ap, FileSystemStorage)
        >>> import os.path
        >>> assert ap.root_dir == os.path.normpath('/foo/bar')
        """
        super(FileSystemStorage, self).__init__(**config)
        assert self.url.startswith(self.__class__.protocol + '://')
        self.root_dir = os.path.normpath(os.path.join(
            self.basedir,
            self.url[len(self.__class__.protocol + '://'):]
        ))
        self.filename_format = config.get('filename_format', '*')
    
    def real_filename(self, filename):
        """Return a filesystem filename from a slash-separated path relative 
        to the access point root.

        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, subdir = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> path = subdir + '/' + basename
        >>> assert ap.real_filename(path) == os.path.normpath(__file__)
        """
        return self.root_dir + os.sep + os.path.normpath(filename.strip('/'))

    def listdir(self, dirname):
        """List files and directories in ``dirname``.
        
        ``dirname`` is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> assert basename in ap.listdir('/')
        """
        return os.listdir(self.real_filename(dirname))

    def open_file(self, filename):
        """Open a file for reading and return a stream.

        ``filename`` is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> # This test searches for itself
        >>> assert 'BdM6Zm62gpYFvGlHuNoS' in ap.open_file(basename).read()
        """
        return open(self.real_filename(filename), 'rb')
        
    def search(self, conditions):
        raise NotImplementedError # TODO
            
    def save(self, item):
        raise NotImplementedError # TODO

    def remove(self, item):
        raise NotImplementedError # TODO
