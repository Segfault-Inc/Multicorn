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
Tests for ReST access point combinations.

"""

import tempfile
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.xml.rest import Rest, RestProperty

from ..test_combinations import FirstAP, SecondAP, teardown_fs


@FirstAP(teardown=teardown_fs)
def first_ap_fs():
    """First access point for Memory."""
    temp_dir = tempfile.mkdtemp()
    fs_properties = [("id", FileSystemProperty(int))]
    fs_ap = FileSystem(
        temp_dir, "(.*)", fs_properties, content_property="stream")
    rest_properties = [
        ("name" , RestProperty(unicode, "//title")),
        ("color" , RestProperty(unicode, "//subtitle")),
        ("second_ap", RestProperty(
                Item, "//paragraph", relation="many-to-one",
                remote_ap="second_ap", remote_property="code"))]
    rest_ap = Rest(fs_ap, rest_properties, "stream")
    return rest_ap

@SecondAP(teardown=teardown_fs)
def second_ap_fs():
    """Second access point for Memory."""
    temp_dir = tempfile.mkdtemp()
    fs_properties = [("code", FileSystemProperty(unicode))]
    fs_ap = FileSystem(
        temp_dir, "(.*)", fs_properties, content_property="stream")
    rest_properties = [
        ("name" , RestProperty(unicode, "//title")),
        ("first_aps", RestProperty(
                iter, "//paragraph/substitution_reference",
                relation="one-to-many", remote_ap="first_ap",
                remote_property="second_ap"))]
    rest_ap = Rest(fs_ap, rest_properties, "stream")
    return rest_ap
