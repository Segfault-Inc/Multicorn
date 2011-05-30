# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Tests for FileSytem access point combinations.

"""

import tempfile
from multicorn.item import Item
from multicorn.access_point.filesystem import FileSystem, FileSystemProperty
from multicorn.access_point.unicode_stream import UnicodeStream

from ..test_combinations import FirstAP, SecondAP, teardown_fs



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
