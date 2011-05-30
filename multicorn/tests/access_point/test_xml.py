# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
XML test.

Test the XML backend.

"""

import tempfile
import shutil
from nose.tools import eq_
from multicorn.access_point.filesystem import FileSystem, FileSystemProperty
from multicorn.access_point.xml import XML, XMLProperty
from multicorn.site import Site

from ..common import run_common, make_site, require



class TemporaryDirectory(object):
    """Utility class for the tests."""
    def __init__(self):
        self.directory = None

    def __enter__(self):
        self.directory = tempfile.mkdtemp()
        return self.directory

    def __exit__(self, exit_type, value, traceback):
        shutil.rmtree(self.directory)


@require("lxml")
def test_serialization():
    """Test XML serialization."""
    def xml_content_test(site):
        """Inner function testing XMD serialization."""
        item = site.open("things", {"id": 1})
        eq_(item["stream"].read().decode("utf-8"),
            "<foo><bar><baz>foo</baz></bar></foo>")
    runner(xml_content_test)

@require("lxml")
def test_update_document():
    """Test document update."""
    def xml_update_test(site):
        """Inner function testing document update."""
        item = site.open("things", {"id": 1})
        item["name"] = "updated"
        item.save()
        item = site.open("things", {"id": 1})
        eq_(item["stream"].read().decode("utf-8"),
            "<foo><bar><baz>updated</baz></bar></foo>")
    runner(xml_update_test)

@require("lxml")
def test_shared_structure():
    """Test item with properties sharing the same structure."""
    with TemporaryDirectory() as temp_dir:
        file_ap = make_file_ap(temp_dir)
        properties = [
            ("name" , XMLProperty(unicode, "//bar/name")),
            ("color", XMLProperty(unicode, "//bar/color"))]
        access_point = XML(file_ap, properties, "stream", "foo")
        site = Site()
        site.register("test", access_point)
        item = site.create("test", {"name": "hulk", "color": "green", "id": 1})
        item.save()
        item = site.open("test", {"id": 1})
        eq_(item["stream"].read().decode("utf-8"),
            "<foo><bar><name>hulk</name><color>green</color></bar></foo>")

@require("lxml")
def test_iter():
    """Test an XML access point with an ``iter`` property."""
    with TemporaryDirectory() as temp_dir:
        file_access_point = make_file_ap(temp_dir)
        access_point = XML(file_access_point, [
                ("name", XMLProperty(iter, "//bar/baz")),], "stream", "foo")
        site = make_site(access_point, fill=False)

        site.create("things", {"id": 1, "name": ("a", "b", "c")}).save()
        item = site.open("things", {"id": 1})
        eq_(tuple(item["name"]), ("a", "b", "c"))
        item = site.open("things", {"name": ("a", "b", "c")})
        eq_(item["id"], 1)

def make_file_ap(temp_dir):
    """Create a filesystem access point."""
    return FileSystem(
        temp_dir, "(.*)\.txt", [("id", FileSystemProperty(int))],
        content_property="stream")


# Common tests

def runner(test):
    """Test runner for ``test``."""
    with TemporaryDirectory() as temp_dir:
        file_access_point = make_file_ap(temp_dir)
        access_point = XML(file_access_point, [
                ("name", XMLProperty(unicode, "//bar/baz")),], "stream", "foo")
        site = make_site(access_point, fill=not hasattr(test, "nofill"))
        test(site)

@run_common
@require("lxml")
def test_xml_common():
    """Define a custom test runner for the common tests."""
    return None, runner, "xml"
