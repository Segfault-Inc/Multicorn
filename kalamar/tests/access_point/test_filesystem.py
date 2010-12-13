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
Filesystem test.

Test the filesystem backend.

"""

import os.path
import tempfile
import shutil
import re
import contextlib
# Nose redefines assert_equal
# pylint: disable=E0611
from nose.tools import eq_, assert_equal, raises
# pylint: enable=E0611

import kalamar
from kalamar.access_point.filesystem import FileSystem, FileSystemProperty
from kalamar.access_point.unicode_stream import UnicodeStream
from kalamar.site import Site
from ..common import run_common, make_site


@contextlib.contextmanager
def temporary_directory():
    """Create a temporary directory.

    This context manager gives the path to a new temporary directory that is
    deleted, with all its content, at the end of the ``with`` block, even if an
    error is raised.

    """
    directory = tempfile.mkdtemp()
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


def test_filesytem_init():
    """Assert that the filesystem access point can be properly initialized."""
    # Project root, contains kalamar dir
    root = os.path.dirname(os.path.dirname(kalamar.__file__))
    access_point = FileSystem(
        root, "(.*)/tests/access_point/test_(.*)\.py(.*)",
        ["package", ("module", FileSystemProperty(unicode)), "extension"])
    site = Site()
    site.register("tests", access_point)
    eq_(set(access_point.properties.keys()),
        set(["package", "module", "extension", "content"]))
    eq_(set((prop.name for prop in access_point.identity_properties)),
        set(["package", "module", "extension"]))
    
    properties = {"package": "kalamar", "module": "filesystem", "extension": ""}
    filename = __file__[:-1] if __file__.endswith(".pyc") else __file__
    eq_(access_point._item_filename(properties), filename)
    
    items_file = site.open("tests", properties)["content"]
    eq_(items_file.name, filename)
    # This test tests its own presence!
    assert "RANDOM STRING A6buCMTbAdCV98j00vK455UIAPCJ" in items_file.read()

def test_temporary_directory():
    """Assert :func:`temporary_directory` works as intented."""
    with temporary_directory() as temp_dir:
        assert os.path.isdir(temp_dir)
        
        # Test that we can write and read files.
        # Maybe this isn’t needed after we asserted isdir(temp_dir).
        filename = os.path.join(temp_dir, "test_file")
        with open(filename, "w") as file_descriptor:
            file_descriptor.write("RANDOM STRING Nj0CmS6GMVwRIhxGQIQy4C")
        with open(filename) as file_descriptor:
            eq_(file_descriptor.read(), "RANDOM STRING Nj0CmS6GMVwRIhxGQIQy4C")
    
    # Make sure every thing is correctly cleaned-up.
    assert not os.path.exists(filename)
    assert not os.path.exists(temp_dir)

@raises(re.error)
def test_filesystem_bad_pattern():
    """Creating an access point with a bad pattern raises an exception."""
    root = os.path.dirname(os.path.dirname(kalamar.__file__))
    FileSystem(
        root, "(.*)/tests/access_point/.*/test_(.*)\.py(",
        ["package", ("module", FileSystemProperty(unicode)), "extension"])

def test_filenames():
    """Assert that the filenames are what we expect."""
    with temporary_directory() as temp_dir:
        access_point = FileSystem(
            temp_dir, "(.*) - (.*)\.(.*)", (
                ("id", FileSystemProperty(int, "%03d")),
                ("title", FileSystemProperty(unicode, "% 6s")),
                ("extension", FileSystemProperty(unicode))),
            content_property="name")
        access_point = UnicodeStream(access_point, "name", "utf-8")
        site = make_site(access_point, fill=False)

        for prop in (
            {"id": 1, "title": "spam", "extension": "txt", "name": ""},
            {"id": 2, "title": "egg", "extension": "text", "name": ""},
            {"id": 3, "title": "turtle", "extension": "rst", "name": ""}):
            site.create("things", prop).save()

        filenames = set([
                u"001 -   spam.txt", u"002 -    egg.text", u"003 - turtle.rst"])

        assert_equal(set(os.listdir(temp_dir)), filenames)
        assert_equal(
            set([item.filename for item in site.search("things")]),
            set([os.path.join(temp_dir, filename) for filename in filenames]))
        assert_equal(
            set([item.relative_filename for item in site.search("things")]),
            filenames)


# Common tests

def runner(test):
    """Test runner for ``test``."""
    with temporary_directory() as temp_dir:
        access_point = FileSystem(
            temp_dir, "(.*)\.txt", [("id", FileSystemProperty(int))],
            content_property="name")
        access_point = UnicodeStream(access_point, "name", "utf-8")
        site = make_site(access_point, fill=not hasattr(test, "nofill"))
        test(site)

@run_common
def test_filesystem_common():
    """Define a custom test runner for the common tests."""
    return None, runner, "Filesystem"
