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
Filesystem
==========

Access point storing items in a filesystem.

"""

import os.path
import re
import io
import shutil

from . import AccessPoint
from ..item import Item
from ..property import Property


class FileSystem(AccessPoint):
    """Store each item in a file."""
    def __init__(self, root_dir, pattern, properties,
                 content_property="content"):
        if pattern.count("*") != len(properties):
            raise ValueError(
                "FileSystem must have as many properties as"
                "* wildcards in pattern.")
        self.root_dir = unicode(root_dir)
        self.content_property = content_property

        self._ordered_properties = tuple(
            (prop, Property(unicode)) if isinstance(prop, basestring)
            else prop # Assume a (name, Property_instance) tuple.
            for prop in properties)

        properties = dict(self._ordered_properties)
        assert content_property not in properties
        properties[content_property] = Property(io.IOBase)
        # All properties here are in the identity
        identity_properties = tuple(
            name for name, prop in self._ordered_properties)
        super(FileSystem, self).__init__(properties, identity_properties)

        pattern_parts = unicode(pattern).split("/")
        props_iter = iter(self.identity_properties)
        self.properties_per_path_part = []
        for part in pattern_parts:
            props = tuple(next(props_iter) for i in xrange(part.count("*")))
            regexp = re.compile(
                "^%s$" % "(.*)".join(
                    re.escape(sub_part) for sub_part in part.split("*")))
            template = part.replace("*", "%s")
            self.properties_per_path_part.append((props, regexp, template))

    def _item_filename(self, item):
        """Item filename."""
        return os.path.join(self.root_dir, *(
                template % tuple(unicode(item[prop]) for prop in props)
                for props, regexp, template in self.properties_per_path_part))

    def search(self, request):
        def defered_open(path):
            """Opener for ``path``."""
            return lambda: (open(path, "rb"),)

        def walk(root, remaining_path_parts, previous_properties=()):
            """Walk through filesystem from ``root`` yielding matching items."""
            props, regexp = remaining_path_parts[0][:2]
            remaining_path_parts = remaining_path_parts[1:]
            for basename in os.listdir(root):
                match = regexp.match(basename)
                if not match:
                    continue
                properties = dict(zip(props, match.groups()))
                properties.update(previous_properties)
                path = os.path.join(root, basename)
                if remaining_path_parts and os.path.isdir(path):
                    for item in walk(path, remaining_path_parts, properties):
                        yield item
                if not remaining_path_parts and not os.path.isdir(path):
                    lazy_loaders = {self.content_property: defered_open(path)}
                    item = Item(self, properties, lazy_loaders)
                    if request.test(item):
                        yield item

        return walk(self.root_dir, self.properties_per_path_part)

    def delete(self, item):
        os.remove(self._item_filename(item))

    def save(self, item):
        content = item[self.content_property]
        if hasattr(content, "seek"):
            content.seek(0)
        with open(self._item_filename(item), "wb") as file_descriptor:
            shutil.copyfileobj(content, file_descriptor)
