# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2010 Kozea
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
import os
import re
import io

from . import AccessPoint
from ..item import Item, AbstractItem
from ..property import Property


# io.IOBase has no __init__ method
# pylint: disable=W0231


class Stream(io.IOBase):
    """Fake stream opening files on demand."""
    def __init__(self, name):
        self.name = name

    def read(self):
        """Read the whole content of the file as bytes."""
        return open(self.name, "rb").read()

    def write(self, bytestring):
        """Write ``bytestring`` into the file."""
        open(self.name, "wb").write(bytestring)

# pylint: enable=W0231


class FileSystemItem(Item):


    @property
    def filename(self):
        return self.access_point._item_filename(self)

    @property
    def relative_filename(self):
        return self.filename.replace(self.access_point.root_dir, '')



class FileSystem(AccessPoint):
    """Store each item in a file."""

    ItemClass = FileSystemItem

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
        transformer = lambda props : (item[prop.name] if prop.type != Item else
                item[prop.name].reference_repr() for prop in props)
        return os.path.join(self.root_dir, *(
                template % tuple(transformer(props))
                for props, regexp, template in self.properties_per_path_part))

    def search(self, request):
        def defered_open(path):
            """Opener for ``path``."""
            return lambda item: (Stream(path),)

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
                    item_properties = {}
                    for prop, value in properties.items():
                        if prop.relation is None:
                            item_properties[prop.name] = value
                        elif prop.relation == 'many-to-one':
                            lazy_loaders[prop.name] = prop.remote_ap.\
                                    loader_from_reference_repr(value)
                        else:
                            lazy_loaders[prop.name] = self._default_loader({}, prop)
                    item_properties = dict(
                        (prop.name, value) for prop, value in properties.items() if prop.relation is None)
                    item = FileSystemItem(self, item_properties, lazy_loaders)
                    item.saved = True
                    if request.test(item):
                        yield item

        return walk(self.root_dir, self.properties_per_path_part)

    def delete(self, item):
        os.remove(self._item_filename(item))

    def save(self, item):
        content = item[self.content_property]
        try:
            content.seek(0)
        except:
            pass
        filename = self._item_filename(item)
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with Stream(self._item_filename(item)) as file_descriptor:
            file_descriptor.write(content.read())
        item.saved = True
