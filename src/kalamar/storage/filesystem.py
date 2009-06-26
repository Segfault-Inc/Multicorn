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
        assert self.url.startswith(self.__class__.protocol + u'://')
        self.root_dir = os.path.normpath(os.path.join(
            self.basedir,
            self.url[len(self.__class__.protocol + u'://'):]
        ))
        self.root_dir = self.root_dir.decode(self.default_encoding)
        self.filename_format = config.get(u'filename_format', u'*')
        self.filename_parts_pattern = []
        index = 1
        for part in self.filename_format.split(u'/'):
            pattern = self._pattern_to_regexp(part, index)
            index += len(pattern.groupindex)
            self.filename_parts_pattern.append(pattern)
    
    def get_storage_properties(self):
        """
        >>> ap = FileSystemStorage(filename_format='*/* - *.mp3', url='file://')
        >>> ap.get_storage_properties()
        ['path1', 'path2', 'path3']
        """
        stars = self.filename_format.count('*')
        return ['path%i' % (i+1) for i in xrange(stars)]
    
    def _real_filename(self, filename):
        """Return a filesystem filename from a slash-separated path relative 
        to the access point root.

        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, subdir = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> path = subdir + '/' + basename
        >>> assert ap._real_filename(path) == os.path.normpath(__file__)
        """
        return self.root_dir + os.sep + os.path.normpath(filename.strip(u'/'))

    def listdir(self, dirname):
        """List files and directories in ``dirname``.
        
        ``dirname`` is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> assert basename in ap.listdir('/')
        """
        return os.listdir(self._real_filename(dirname))

    def isdir(self, dirname):
        """Return true if ``dirname`` refers to an existing directory.
        
        ``dirname`` is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, basename = os.path.split(dirname) # .../kalamar, storage
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> assert ap.isdir(basename)
        """
        return os.path.isdir(self._real_filename(dirname))

    def open_file(self, filename, mode='rb'):
        """Open a file for reading and return a stream.

        ``filename`` is a slash-separated path relative to the access point
        root.
        
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(url='file://' + dirname)
        >>> # This test searches for itself
        >>> assert 'BdM6Zm62gpYFvGlHuNoS' in ap.open_file(basename).read()
        """
        return open(self._real_filename(filename), mode)
        
    def rename(self, source, dest):
        """Rename/move a file.

        ``filename`` is a slash-separated path relative to the access point
        root.
        """
        os.renames(self._real_filename(source),
                   self._real_filename(dest))

    def remove_file(self, filename):
        """Remove a file from the backend

        ``filename`` is a slash-separated path relative to the access point
        root.
        """
        os.remove(self._real_filename(filename))

    @staticmethod
    def _pattern_to_regexp(pattern, first_index):
        r"""
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
        """
        >>> dirname, module = os.path.split(__file__)
        >>> dirname, package = os.path.split(dirname) # .../kalamar, storage
        >>> ap = AccessPoint.from_url(url='file://' + dirname,
        ...                           filename_format='*/*.py')
        >>> from kalamar.site import Site
        >>> req = list(Site.parse_request('path1=storage/path2~!=^__'))
        >>> [1 for props, opener in ap._storage_search(req)
        ...    if props['path2'].startswith('__')]
        []
        >>> [1 for props, opener in ap._storage_search(req)
        ...    if props['path1'] != 'storage']
        []
        >>> [1 for props, opener in ap._storage_search(req)
        ...    if props == {'path1': 'storage', 'path2': 'filesystem'}]
        [1]
        """
        conditions = list(conditions)
        
        def walk(subdir, pattern_parts, previous_properties):
            pattern = pattern_parts[0]
            pattern_parts = pattern_parts[1:]
            
            for name in self.listdir(subdir):
                match = pattern.match(name)
                if match:
                    # the name matches the pattern, extract the values
                    properties = match.groupdict()
                    for prop_name, operator, value in conditions:
                        if prop_name in properties:
                            if not operator(properties[prop_name], value):
                                break
                    else:
                        # all the conditions for the present properties are met
                        path = subdir + u'/' + name
                        properties.update(previous_properties)
                        if self.isdir(path):
                            if pattern_parts:
                                # this is a directory and the filename
                                # pattern has more parts
                                for result in walk(path, pattern_parts,
                                                   properties):
                                    yield result
                        else:
                            if not pattern_parts:
                                # this is a file and the filename
                                # pattern is completed
                                yield properties, FileOpener(
                                    self._real_filename(path)
                                )
        
        return walk(u'', self.filename_parts_pattern, ())
            
    
    def _path_from_properties(self, properties):
        """
        Rebuild a path from a dict of properties
        The path is as accepted by AccessPoint.open_file etc.
        
        >>> ap = AccessPoint.from_url(url='file://', filename_format='*/*.py')
        >>> ap._path_from_properties({'path1': 'storage', 'path2': 'fs'})
        'storage/fs.py'
        """
        def path_parts(pattern_parts):
            yield pattern_parts[0]
            for num, part in enumerate(pattern_parts[1:]):
                yield properties['path%i' % (num + 1)]
                yield part
        return ''.join(path_parts(self.filename_format.split('*')))

        
    def save(self, item):
        """
        Add/update/move an item
        """
        old_path = self._path_from_properties(
            item.properties.storage_properties_old
        )
        new_path = self._path_from_properties(
            item.properties.storage_properties
        )
        move = old_path != new_path
        change = item.properties.content_modified
        if change:
            if move:
                # remove old_path
                self.remove(item)
            f = self.open_file(new_path, mode='wb')
            f.write(item.serialize())
            f.close()
        elif move:
            self.rename(old_path, new_path)

    def remove(self, item):
        """
        Remove the given item from the backend storage
        """
        self.remove_file(self._path_from_properties(
            item.properties.storage_properties_old
        ))

