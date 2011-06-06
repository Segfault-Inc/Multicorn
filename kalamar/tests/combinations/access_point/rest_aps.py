# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for ReST access point combinations.

"""

import tempfile
from kalamar.item import Item
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.xml.rest import Rest, RestProperty

from ..test_combinations import FirstAP, SecondAP, teardown_fs
from ...common import require



@FirstAP(teardown=teardown_fs)
@require("docutils")
@require("lxml")
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
@require("docutils")
@require("lxml")
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
