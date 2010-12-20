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
Tests for XML access point combinations.

"""

import tempfile
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.xml import XML, XMLProperty

from ..test_combinations import FirstAP, SecondAP, teardown_fs



@FirstAP(teardown=teardown_fs)
def first_ap_fs():
    """First access point for Memory."""
    temp_dir = tempfile.mkdtemp()
    fs_properties = [("id", FileSystemProperty(int))]
    fs_ap = FileSystem(
        temp_dir, "(.*)", fs_properties, content_property="stream")
    xml_properties = [
        ("name" , XMLProperty(unicode, "//name")),
        ("color" , XMLProperty(unicode, "//color")),
        ("second_ap", XMLProperty(
                Item, "//second_ap", relation="many-to-one",
                remote_ap="second_ap", remote_property="code"))]
    xml_ap = XML(fs_ap, xml_properties, "stream", "root")
    return xml_ap

@SecondAP(teardown=teardown_fs)
def second_ap_fs():
    """Second access point for Memory."""
    temp_dir = tempfile.mkdtemp()
    fs_properties = [("code", FileSystemProperty(unicode))]
    fs_ap = FileSystem(
        temp_dir, "(.*)", fs_properties, content_property="stream")
    xml_properties = [
        ("name", XMLProperty(unicode, "//name")),
        ("first_aps", XMLProperty(
                iter, "//first_aps", relation="one-to-many",
                remote_ap="first_ap", remote_property="second_ap"))]
    xml_ap = XML(fs_ap, xml_properties, "stream", "root")
    return xml_ap
