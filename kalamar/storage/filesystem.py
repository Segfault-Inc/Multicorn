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
import re
import functools
import werkzeug
from random import random

from kalamar import utils
from kalamar.storage.base import AccessPoint
from kalamar.item import Item



class FileSystemStorage(AccessPoint):
    """Store items in files."""
    protocol = 'file'
    
    def __init__(self, config):
        """Initialize the storage according to the given configuration.


        Fixture
        >>> import os.path
        >>> from kalamar.config import Config
        >>> ap = AccessPoint.from_url(Config('file://bar','',{}, basedir='/foo'))
        
        Test
        >>> assert ap.root == os.path.normpath('/foo/bar')
        >>> assert isinstance(ap, FileSystemStorage)

        """
        super(FileSystemStorage, self).__init__(config)

        self.root = os.path.normpath(os.path.join(
            self.basedir or '',
            self.url[len(self.__class__.protocol + '://'):]))

        self.filename_format = config.additional_properties.get('filename_format', '*')

    def get_storage_properties(self):
        """Return a list of the properties used for the storage.
        >>> from kalamar.config import Config
        >>> ap = FileSystemStorage(Config('file://','',{'filename_format':'*/* - *.mp3'}))
        >>> ap.get_storage_properties()
        ['path1', 'path2', 'path3']

        """
        stars = self.filename_format.count('*')
        return ['path%i' % (i + 1) for i in xrange(stars)]
            
    @property
    def primary_keys(self):
        """List of path* keys."""
        return self.get_storage_properties()
    
    def _real_filename(self, filename):
        """Return a filesystem filename from a slash-separated path relative 
        to the access point root.

        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, subdir = os.path.split(dirname)
        >>> from kalamar.config import Config
        >>> ap = AccessPoint.from_url(Config('file://%s' % dirname,'',{}))
        >>> path = '%s/%s' % (subdir, basename)
        >>> assert ap._real_filename(path) == os.path.normpath(__file__)

        """
        return os.path.join(self.root, *filename.split('/'))

    def listdir(self, dirname):
        """List files and directories in ``dirname``.
    
        ``dirname`` is a slash-separated path relative to the access point
        root.
        
        >>> from kalamar.config import Config
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(Config('file://' + dirname,'',{}))
        >>> assert basename in ap.listdir(u'/')
        >>> assert isinstance(ap.listdir(u'/')[0], unicode)

        """
        return os.listdir(self._real_filename(dirname))

    def isdir(self, dirname):
        """Return true if ``dirname`` refers to an existing directory.
        
        ``dirname`` is a slash-separated path relative to the access point
        root.
        >>> from kalamar.config import Config
        >>> dirname, basename = os.path.split(__file__)
        >>> dirname, basename = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(Config('file://%s' % dirname,'',{}))
        >>> assert ap.isdir(basename)

        """
        return os.path.isdir(self._real_filename(dirname))

    def open_file(self, filename, mode='rb'):
        """Open a file for reading and return a stream.

        ``filename`` is a slash-separated path relative to the access point
        root.
        
        If opening for writing (ie. 'w' in mode), create parent directories
        as needed.
        >>> from kalamar.config import Config
        >>> dirname, basename = os.path.split(__file__)
        >>> ap = AccessPoint.from_url(Config('file://%s' % dirname,'',{}))
        >>> # This test searches for itself
        >>> assert 'BdM6Zm62gpYFvGlHuNoS' in ap.open_file(basename).read()

        """
        if 'w' in mode and '/' in filename:
            head = filename.rsplit('/', 1)[0]
            if head:
                real_head = self._real_filename(head)
                # TODO: test parent directories creation
                if not os.path.exists(real_head):
                    os.makedirs(real_head)
        return open(self._real_filename(filename), mode)
    
    def get_file_content(self, filename, real_filename=False):
        if real_filename:
            file_descriptor = open(filename, 'rb')
        else:
            file_descriptor = self.open_file(filename)
        data = file_descriptor.read()
        file_descriptor.close()
        return data
        
    def rename(self, source, destination):
        """Rename/move a file from ``source`` to ``detination``.

        ``source`` and ``destination `` are slash-separated paths relative to
        the access point root.

        """
        os.renames(self._real_filename(source),
                   self._real_filename(destination))

    def remove_file(self, filename):
        """Remove a file from the backend.

        ``filename`` is a slash-separated path relative to the access point
        root.

        """
        os.remove(self._real_filename(filename))

    @staticmethod
    def _pattern_to_regexp(pattern, first_index):
        r"""Transform a standard pattern with wildcards into a regexp object.

        >>> re_ = FileSystemStorage._pattern_to_regexp(u'[*] * - *.txt', 3)
        >>> print re_
        ^\[(?P<path3>.*)\]\ (?P<path4>.*)\ \-\ (?P<path5>.*)\.txt$
        >>> re_ = re.compile(re_)
        >>> re_ # doctest: +ELLIPSIS
        <_sre.SRE_Pattern object at 0x...>
        >>> re_.match(u'[au!] ... - .txt').groups()
        (u'au!', u'...', u'')
        >>> # Missing ' ' after ']'
        >>> assert re_.match(u'[au!]... - Ô.txt') is None

        """
        def regexp_parts():
            pattern_parts = pattern.split('*')
            yield '^'
            yield re.escape(pattern_parts[0])
            for num, part in enumerate(pattern_parts[1:]):
                yield '(?P<path%i>.*)' % (first_index + num)
                yield re.escape(part)
            yield '$'

        return ''.join(regexp_parts())
    
    def _storage_search(self, conditions):
        """Generate (properties, file_opener) for all files matching conditions.
        >>> from kalamar.config import Config
        >>> dirname, module = os.path.split(__file__)
        >>> dirname, package = os.path.split(dirname)
        >>> ap = AccessPoint.from_url(Config('file://%s' % dirname,'',{'filename_format':'*/*.py'}))
        >>> from kalamar.site import Site
        >>> request = list(Site.parse_request('path1="storage"/path2~!="^__"'))
        >>> len([1 for properties, opener in ap._storage_search(request)
        ...      if properties['path2'].startswith('__')])
        0
        >>> len([1 for properties, opener in ap._storage_search(request)
        ...     if properties['path1'] != 'storage'])
        0
        >>> len([1 for properties, opener in ap._storage_search(request)
        ...     if (properties['path1'], properties['path2']) == 
        ...        ('storage', 'filesystem')])
        1
        >>> request = Site.parse_request('path1="parser"/path2~="item$"')
        >>> results = [properties['path2']
        ...            for properties, opener in ap._storage_search(request)]
        >>> assert u'textitem' in results
        >>> assert u'__init__' not in results

        """
        conditions = list(conditions)
        
        def walk(subdir, pattern_parts, previous_properties):
            """Walk through ``subdir`` finding files matching ``pattern_parts``.

            Generate (``properties``, ``file_opener``) for files in ``subdir``
            matching ``pattern_parts``, and recursively call ``walk`` with
            subdirs in subdir.

            """
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
                                filename = self._real_filename(path)
                                yield properties, functools.partial(
                                    self.get_file_content, path)
        
        return walk(u'', self.filename_pattern_parts, ())
            
    def _path_from_properties(self, properties):
        """Rebuild a path from a dict of properties.

        The path is as accepted by AccessPoint.open_file.
        
        >>> from kalamar.config import Config
        >>> ap = AccessPoint.from_url(Config(u'file://','',{"filename_format":u'*/*.py'}))
        >>> ap._path_from_properties({u'path1': u'storage', u'path2': u'fs'})
        u'storage/fs.py'

        """
        def path_parts():
            pattern_parts = self.filename_format.split('*')
            yield pattern_parts[0]
            for i, part in enumerate(pattern_parts[1:]):
                # If no property is set, give a random name instead of None
                # Closes bug #8
                # TODO avoid already existing numbers (``hash`` limitation)
                yield unicode(properties[u'path%i' % (i + 1)]
                              or abs(hash(random())))
                yield part

        return ''.join(path_parts())

    def save(self, item):
        """Add/update/move an item."""
        # Storage properties must be unicode strings
        for name in self.get_storage_properties():
            if item[name] is not None:
                item[name] = unicode(item[name])

        if item.old_storage_properties:
            old_path = self._path_from_properties(item.old_storage_properties)
        else:
            old_path = None

        new_path = self._path_from_properties(item.raw_storage_properties)

        if item.modified:
            if old_path and not item.parser_modified:
                # Move only: just rename
                self.rename(old_path, new_path)
            else:
                # Add or update: serialize content…
                content = item.serialize()

                if old_path and item.storage_modified:
                    # Move and update: remove old file
                    self.remove(item)

                # Add or update: …and write it
                file_descriptor = self.open_file(new_path, mode='wb')
                file_descriptor.write(content)
                file_descriptor.close()

    def remove(self, item):
        """Remove ``item`` from the backend storage."""
        self.remove_file(self._path_from_properties(
            item.old_storage_properties))
            
    def filename_for(self, item):
        """Return the real filename for ``item``."""
        return self._real_filename(self._path_from_properties(
            item.raw_storage_properties))
    
    @werkzeug.cached_property
    @utils.apply_to_result(list)
    def filename_pattern_parts(self):
        index = 1
        for part in self.filename_format.split('/'):
            yield re.compile(self._pattern_to_regexp(part, index))
            index += part.count('*')
    
    def item_from_filename(self, filename):
        """
        Search for an item matching this ``filename``.
        ``filename`` has to be os.normpath’d.

        >>> from kalamar.config import Config
        >>> dirname, module = os.path.split(__file__)
        >>> dirname, package = os.path.split(dirname)
        >>> dirname = os.path.normpath(dirname)
        
        ``dirname`` is the path to the kalamar package.
        
        >>> ap = AccessPoint.from_url(Config('file://%s' % dirname,None,{'filename_format':'*/*.py'},parser='text'))
        >>> search = ap.item_from_filename
        
        # all these should return None
        >>> search('/foo/bar') # do not start with self.root \
        # do not match the pattern
        >>> search(os.path.join(dirname, 'inexistent.py'))
        >>> search(os.path.join(dirname, 'site.py'))
        >>> search(os.path.join(dirname, 'storage', 'inexistent.pyc'))
        >>> search(os.path.join(dirname, 'storage', 'base.pyc')) \
        # matches, but does not exist
        >>> search(os.path.join(dirname, 'storage', 'inexistent.py'))
        
        >>> item = search(os.path.join(dirname, 'storage', 'filesystem.py'))
        >>> item # doctest: +ELLIPSIS
        <TextItem(u"path1='storage'/path2='filesystem'" @ None)>
        >>> item['path1']
        'storage'
        >>> item['path2']
        'filesystem'
        >>> item.filename == \
                os.path.normpath(os.path.splitext(__file__)[0] + '.py')
        True

        """
        absolute_filename = os.path.abspath(filename)
        root = os.path.abspath(self.root)
        if not absolute_filename.startswith(root):
            return None
        
        # relative to the access point root
        relative_filename = absolute_filename[len(root):]
        
        parts = [part for part in relative_filename.split(os.path.sep) if part]
        if len(self.filename_pattern_parts) != len(parts):
            return None
        
        properties = {}

        for part, regexp in zip(parts, self.filename_pattern_parts):
            match = regexp.match(part)
            if not match:
                return None
            properties.update(match.groupdict())
            
        if not os.path.isfile(filename):
            return None

        return self._make_item(
            functools.partial(
                self.get_file_content, filename, real_filename=True),
            properties)
