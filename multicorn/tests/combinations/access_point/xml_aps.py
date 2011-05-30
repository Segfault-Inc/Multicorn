# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for XML access point combinations.

"""

import tempfile
from multicorn.item import Item
from multicorn.access_point.filesystem import FileSystem, FileSystemProperty
from multicorn.access_point.xml import XML, XMLProperty

from ..test_combinations import FirstAP, SecondAP, teardown_fs
from ...common import require



@FirstAP(teardown=teardown_fs)
@require("lxml")
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
@require("lxml")
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
