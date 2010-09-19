# -*- coding: utf-8 -*-
# This file is part of Dyko
# Copyright © 2008-2009 Kozea
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
Property test
=============

Test the Property class.

"""

from nose.tools import eq_, raises

from . import test_memory
from ..property import Property, MissingRemoteAP, MissingRemoteProperty


def test_property_creation():
    remote_ap = test_memory.make_test_site().access_points["things"]
    prop = Property(str)
    eq_(prop.type, str)
    prop = Property(int, True, True, 42, True, "many-to-one", remote_ap, "name")
    eq_(prop.type, int)

@raises(MissingRemoteAP)
def test_property_creation_missing_remote_ap():
    prop = Property(float, relation="many-to-one")

@raises(MissingRemoteProperty)
def test_property_creation_missing_remote_property():
    remote_ap = test_memory.make_test_site().access_points["things"]
    prop = Property(float, relation="one-to-many", remote_ap=remote_ap)

@raises(RuntimeError)
def test_property_creation_missing_remote_property_and_ap():
    prop = Property(float, relation="one-to-many")
