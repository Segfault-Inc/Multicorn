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
# along with Kalamar.  If not, see <http://www.gnu.org/licenses/>.

"""
Filesystem access point.

This implementation relies on default python filesystem functions and should
work on all platforms.

"""

import os
import glob
import re
import itertools

from kalamar.storage.base import AccessPoint

class FileOpener(object):
    def __init__(self, filename):
        self.filename = filename

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.filename)

    def __eq__(self, other):
        return self.filename == other.filename

    def __call__(self):
        return open(self.filename, 'rb')
        

class FileSystemStorage(AccessPoint):
    """Store items in files."""
    
    protocol = 'file'
    
    def __init__(self, **config):
        """Initialize the storage according to the given configuration.

        >>> ap = AccessPoint.from_url(basedir='/foo', url='file://bar')
        >>> assert isinstance(ap, FileSystemStorage)
        >>> import os.path
        >>> assert ap.root == os.path.normpath('/foo/bar')

        """
        super(FileSystemStorage, self).__init__(**config)

        self.root = os.path.normpath(os.path.join(
            self.basedir,
            self.url[len(self.__class__.protocol + '://'):]))

        index = 1
        self.filename_format = config.get('filename_format', '*')
        self.filename_parts_pattern = []
        for part in self.filename_format.split('/'):
            pattern = self._pattern_to_regexp(part, index)
            index += len(pattern.groupindex)
            self.filename_parts_pattern.append(pattern)
    
    def get_storage_properties(self):
        """Return a list of the properties used for the storage.

        >>> ap = FileSystemStorage(filename_format='*/* - *.mp3', url='file://')
        >>> ap.get_storage_properties()
        ['path1', 'path2', 'path3']

        """
        stars = self.filename_format.count('*')
        return ['path%i' % (i + 1) for i in xrange(stars)]
    
    def _real_filename(self, filename):
        """Return a filesystem filename from a slash-separated path relative 
        to the access point root.

        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, subdir = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(url='file://%s' % dirname)
        >>> path = '%s/%s' % (subdir, basename)
        >>> assert ap._real_filename(path) == os.path.normpath(__file__)

        """
        return os.path.join(self.root, *filename.split('/'))

    def listdir(self, dirname):
        """List files and directories in "dirname".
        
        "dirname" is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> assert basename in ap.listdir(u'/')
        >>> assert isinstance(ap.listdir(u'/')[0], unicode)

        """
        return os.listdir(self._real_filename(dirname))

    def isdir(self, dirname):
        """Return true if "dirname" refers to an existing directory.
        
        "dirname" is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, basename = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(url='file://%s' % dirname)
        >>> assert ap.isdir(basename)

        """
        return os.path.isdir(self._real_filename(dirname))

    def open_file(self, filename, mode='rb'):
        """Open a file for reading and return a stream.

        "filename" is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(url='file://%s' % dirname)
        >>> # This test searches for itself
        >>> assert 'BdM6Zm62gpYFvGlHuNoS' in ap.open_file(basename).read()

        """
        return open(self._real_filename(filename), mode)
        
    def rename(self, source, destination):
        """Rename/move a file.

        "filename" is a slash-separated path relative to the access point
        root.

        """
        os.renames(self._real_filename(source),
                   self._real_filename(destination))

    def remove_file(self, filename):
        """Remove a file from the backend

        "filename" is a slash-separated path relative to the access point
        root.

        """
        os.remove(self._real_filename(filename))

    @staticmethod
    def _pattern_to_regexp(pattern, first_index):
        r"""Transform the standard pattern with wildcards to a real regexp.

        >>> re_ = FileSystemStorage._pattern_to_regexp('[*] * - *.txt', 3)
        >>> re_ # doctest: +ELLIPSIS
        <_sre.SRE_Pattern object at 0x...>
        >>> re_.match('[au!] ... - .txt').groups()
        ('au!', '...', '')
        >>> assert re_.match('[au!]... - Ô.txt') is None
        >>> re_.pattern
        '^\\[(?P<path3>.*)\\]\\ (?P<path4>.*)\\ \\-\\ (?P<path5>.*)\\.txt$'

        """
        def regexp_parts(pattern_parts):
            yield '^'
            yield re.escape(pattern_parts[0])
            for num, part in enumerate(pattern_parts[1:]):
                yield '(?P<path%i>.*)' % (first_index + num)
                yield re.escape(part)
            yield '$'

        return re.compile(''.join(regexp_parts(pattern.split('*'))))
    
    def _storage_search(self, conditions):
        """Generate (properties, file_opener) for all files matching conditions.

        >>> dirname, module = os.path.split(__file__)
        >>> dirname, package = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(url='file://%s' % dirname,
        ...                           filename_format='*/*.py')
        >>> from kalamar.site import Site
        >>> request = list(Site.parse_request('path1=storage/path2~!=^__'))
        >>> [1 for properties, opener in ap._storage_search(request)
        ...    if properties['path2'].startswith('__')]
        []
        >>> [1 for properties, opener in ap._storage_search(request)
        ...    if properties['path1'] != 'storage']
        []
        >>> [1 for properties, opener in ap._storage_search(request)
        ...    if properties == {'path1': 'storage', 'path2': 'filesystem'}]
        [1]

        """
        conditions = list(conditions)
        
        def walk(subdir, pattern_parts, previous_properties):
            """Generate (properties, file_opener) for files in "subdir"
            matching "pattern_parts", and recursively call "walk" with subdirs
            in subdir."""
            # We make a copy of pattern_parts here instead of using
            # pattern_parts.pop(0) because the original list may be used
            # for other calls to walk()
            pattern = pattern_parts[0]
            pattern_parts = pattern_parts[1:]
            
            for name in self.listdir(subdir):
                match = pattern.match(name)
                if match:
                    # The name matches the pattern, extract the values
                    properties = match.groupdict()
                    for condition in conditions:
                        if condition.property_name in properties:
                            if not condition.operator(
                                properties[condition.property_name], 
                                condition.value):
                                break
                    else:
                        # All the conditions for the present properties are met
                        path = u'%s/%s' % (subdir, name)
                        properties.update(previous_properties)
                        if self.isdir(path):
                            if pattern_parts:
                                # This is a directory and the filename
                                # pattern has more parts
                                for result in walk(path, pattern_parts,
                                                   properties):
                                    yield result
                        else:
                            if not pattern_parts:
                                # This is a file and the filename
                                # pattern is completed
                                yield properties, FileOpener(
                                    self._real_filename(path))
        
        return walk(u'', self.filename_parts_pattern, ())
            
    
    def _path_from_properties(self, properties):
        """Rebuild a path from a dict of properties.

        The path is as accepted by AccessPoint.open_file.
        
        >>> ap = AccessPoint.from_url(url='file://', filename_format='*/*.py')
        >>> ap._path_from_properties({'path1': 'storage', 'path2': 'fs'})
        'storage/fs.py'

        """
        def path_parts(pattern_parts):
            """TODO: Document this"""
            yield pattern_parts[0]
            for i, part in enumerate(pattern_parts[1:]):
                yield properties['path%i' % (i + 1)]
                yield part

        return ''.join(path_parts(self.filename_format.split('*')))

        
    def save(self, item):
        """Add/update/move an item."""
        old_path = self._path_from_properties(
            item.properties.storage_properties_old)
        new_path = self._path_from_properties(
            item.properties.storage_properties)

        move = old_path != new_path
        change = item.properties.parser_content_modified
        if change:
            if move:
                # Remove old_path
                self.remove(item)
            fd = self.open_file(new_path, mode='wb')
            fd.write(item.serialize())
            fd.close()
        elif move:
            self.rename(old_path, new_path)

    def remove(self, item):
        """Remove the given item from the backend storage."""
        self.remove_file(self._path_from_properties(
            item.properties.storage_properties_old))

