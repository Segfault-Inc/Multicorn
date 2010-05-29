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
General functions to generate random objects of different types.

Most of the generated objects are based on the UUID mechanism.

"""

import uuid

# TODO: test these functions

def random_long(maximum=None):
    """Gererate random positive long integer, between 0 and `maximum`."""
    value = uuid.uuid4().int
    return value % maximum if maximum else value

def random_int(maximum=None):
    """Gererate random positive int, between 0 and `maximum`."""
    return int(random_long(maximum))

def random_str(length=None):
    """Gererate a random string, with maximum `length`."""
    string = str(uuid.uuid4())
    return string[:length] if len(string) > length else string

def random_timestamp():
    """Gererate a random timestamp."""
    return uuid.uuid4().time

def random_bool():
    """Generate a random boolean."""
    return bool(random_long(2))
