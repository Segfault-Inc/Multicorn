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


def regexp_to_template(regexp):
    """Transform a regexp to a python-formatted unicode.

    >>> regexp_to_template(re.compile("spam"))
    u'spam'
    >>> regexp_to_template(re.compile("egg(.*)"))
    u'egg%s'
    >>> regexp_to_template(re.compile(u"☺(.*)"))
    u'\\xe2\\x98\\xba%s'
    >>> regexp_to_template(re.compile("egg(.*)\\.test"))
    u'egg%s.test'
    >>> regexp_to_template(re.compile("\\(egg(.*)\\)\\.test\\\\\\\\"))
    u'(egg%s).test\\'

    """
    parts = [part.split(")")[-1] for part in regexp.pattern.split("(")]
    return u"%s".join(parts).replace("\\", "").strip("^$")


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
    """Item stored as a file."""
    @property
    def filename(self):
        return self.access_point._item_filename(self)

    @property
    def relative_filename(self):
        return self.filename.replace(self.access_point.root_dir, "")


class FileSystem(AccessPoint):
    """Store each item in a file."""
    ItemClass = FileSystemItem

    def __init__(self, root_dir, pattern, properties,
                 content_property="content"):
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

        pattern_parts = pattern.split("/")
        props_iter = iter(self.identity_properties)
        self.properties_per_path_part = []
        for part in pattern_parts:
            regexp = re.compile(u"^%s$" % part)
            props = tuple(next(props_iter) for i in xrange(regexp.groups))
            self.properties_per_path_part.append((props, regexp))

    def _item_filename(self, item):
        """Item filename."""
        transformer = lambda props: (item[prop.name] if prop.type != Item else
                item[prop.name].reference_repr() for prop in props)
        return os.path.join(self.root_dir, *(
                regexp_to_template(regexp) % tuple(transformer(props))
                for props, regexp in self.properties_per_path_part))

    def search(self, request):
        def defered_open(path):
            """Opener for ``path``."""
            return lambda item: (Stream(path),)

        def walk(root, remaining_path_parts, previous_properties=()):
            """Walk through filesystem from ``root`` yielding matching items."""
            props, regexp = remaining_path_parts[0]
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
                            lazy_loaders[prop.name] = \
                                prop.remote_ap.loader_from_reference_repr(value)
                        else:
                            lazy_loaders[prop.name] = \
                                self._default_loader({}, prop)
                    item_properties = dict(
                        (prop.name, value) for prop, value
                        in properties.items() if prop.relation is None)
                    item = FileSystemItem(self, item_properties, lazy_loaders)
                    item.saved = True
                    if request.test(item):
                        yield item

        return walk(self.root_dir, self.properties_per_path_part)

    def delete(self, item):
        filename = self._item_filename(item)
        os.remove(filename)
        basedir = os.path.dirname(filename)
        if not os.listdir(basedir):
            os.removedirs(basedir)

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
