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
Property test.

Test the Property class.

"""

from nose.tools import eq_, raises

from kalamar.access_point.memory import Memory
from kalamar.property import Property, MissingRemoteAP, MissingRemoteProperty, \
    AlreadyRegistered
from .common import make_site



def memory_make_ap():
    """Create a simple access point."""
    return Memory({"id": Property(int), "name": Property(unicode)}, ("id",))

# Some properties are just used to test equality
# pylint: disable=C0103

# Function name are quite long but explicit
# pylint: disable=W0612

def test_property_creation():
    """Test the creation of access point properties."""
    remote_ap = make_site(
        memory_make_ap(), fill=True).access_points["things"]
    prop = Property(unicode)
    eq_(prop.type, unicode)
    prop = Property(
        int, True, True, lambda prop: 42, "many-to-one", remote_ap, "name")
    eq_(prop.type, int)

@raises(MissingRemoteAP)
def test_property_creation_missing_remote_ap():
    """Check that property for a missing access point raises an exception."""
    prop = Property(float, relation="many-to-one")

@raises(MissingRemoteProperty)
def test_property_creation_missing_remote_property():
    """Check that a property missing for an access point raises an exception."""
    remote_ap = make_site(
        memory_make_ap(), fill=True).access_points["things"]
    prop = Property(float, relation="one-to-many", remote_ap=remote_ap)

@raises(RuntimeError)
def test_property_creation_missing_remote_property_and_ap():
    """Check that a one-to-many prop missing prop and ap raises an exception."""
    prop = Property(float, relation="one-to-many")

@raises(AlreadyRegistered)
def test_already_registered():
    """Try to register a property twice."""
    prop = Property(unicode)

    remote_ap = make_site(
        memory_make_ap(), fill=True).access_points["things"]
    remote_ap.register("test", prop)
    remote_ap = make_site(
        memory_make_ap(), fill=True).access_points["things"]
    remote_ap.register("eggs", prop)

# pylint: enable=C0103
# pylint: enable=W0612
