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
Tests for FileSytem access point combinations.

"""

import tempfile
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.unicode_stream import UnicodeStream

from ..test_combinations import FirstAP, SecondAP, teardown_fs

if "unicode" not in locals():
    unicode = str


@FirstAP(teardown=teardown_fs)
def first_ap_fs():
    """First access point for Memory."""
    temp_dir = tempfile.mkdtemp()
    fs_properties = [
        ("id", FileSystemProperty(int)),
        ("name", FileSystemProperty(unicode)),
        ("second_ap", FileSystemProperty(
                Item, relation="many-to-one", remote_ap="second_ap",
                remote_property="code"))]
    fs_ap = FileSystem(
        temp_dir, "(.*)/(.*)/(.*)", fs_properties, content_property="color")
    return UnicodeStream(fs_ap, "color", "utf-8")

@SecondAP(teardown=teardown_fs)
def second_ap_fs():
    """Second access point for Memory."""
    temp_dir = tempfile.mkdtemp()
    remote_property = FileSystemProperty(
        iter, relation="one-to-many", remote_ap="first_ap",
        remote_property="second_ap")
    fs_properties = [
        ("code", FileSystemProperty(unicode)),
        ("first_aps", remote_property)]
    fs_ap = FileSystem(
        temp_dir, "(.*)", fs_properties, content_property="name")
    return UnicodeStream(fs_ap, "name", "utf-8")
