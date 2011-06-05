# -*- coding: utf-8 -*-
# Copyright © 2008-2011 Kozea
# This file is part of Multicorn, licensed under 3-clause BSD

"""
Item test.

Test the Item class.

"""

import sys
from nose.plugins.skip import SkipTest

from multicorn.access_point.memory import Memory
from multicorn.property import Property
from .common import make_site
from multicorn.item import MultiDict



def memory_make_ap():
    """Create a simple access point."""
    return Memory({"id": Property(int), "name": Property(unicode)}, ("id",))

def test_modification_tracking():
    """Test the modification tracking system."""
    # Some statements seem useless here, but they are useful
    # pylint: disable=W0104
    site = make_site(memory_make_ap(), fill=True)
    item = tuple(site.search("things"))[0]
    assert not item.modified
    item["name"] = "spam"
    assert item.modified
    item.save()
    assert not item.modified
    item["name"]
    assert not item.modified
    item.getlist("name")
    assert not item.modified
    item.setlist("name", ("spam", "egg"))
    assert item.modified
    item.save()
    assert not item.modified
    # pylint: enable=W0104

def test_item_representation():
    """Test the representation of item references."""
    site = make_site(memory_make_ap(), fill=True)
    item = tuple(site.search("things"))[0]
    assert item.reference_repr() == "2"


def test_update_self():
    """Test `MutableMultiMapping.update` with a keword argument named `self`"""
    if sys.version_info < (2, 7, 0, 'alpha', 3):
        # This test depends on http://bugs.python.org/issue9137 being fixed.
        # The fix was fist released in CPython 2.7.0a3:
        # http://svn.python.org/projects/python/tags/r271/Misc/NEWS
        raise SkipTest
    dict_ = MultiDict()
    dict_.update(self=4)
    assert dict_ == {'self': 4}

